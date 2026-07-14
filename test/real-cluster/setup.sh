#!/usr/bin/env bash
# Bring up the real 3-node test cluster (mario=control+worker, luigi/peach=workers),
# deploy the go-hello fixture and publish it on $APP_DOMAIN with a Let's Encrypt cert.
#
# Idempotent: re-run at any time; run a single stage with `./setup.sh <stage>`.
# Stages: keys docker build install control login workers catrust deploy verify

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

BOOTSTRAP_ENV="$STATE_DIR/bootstrap.env"
JOIN_TOKEN_FILE="$STATE_DIR/join.token"
CLUSTER_CA="$STATE_DIR/cluster-ca.crt"

# ---------------------------------------------------------------- stage: keys
stage_keys() { ensure_ssh_access; }

# -------------------------------------------------------------- stage: docker
stage_docker() {
  local node
  for node in "${NODES[@]}"; do
    log "[$node] preparing host (firewall, docker)"
    zssh "$node" '
      set -e
      # No host firewall on the test cluster: zattera needs 80,443,8443,5000/tcp + 51820/udp.
      if systemctl is-active --quiet firewalld 2>/dev/null; then systemctl disable --now firewalld; fi
      if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -q "Status: active"; then ufw disable; fi
      if ! command -v docker >/dev/null 2>&1; then
        curl -fsSL https://get.docker.com | sh
      fi
      systemctl enable --now docker
      docker info --format "{{.ServerVersion}}" >/dev/null
    '
    log "[$node] docker $(zssh "$node" docker --version | tr -d '\r')"
  done
}

# --------------------------------------------------------------- stage: build
stage_build() {
  local node arch archs=""
  for node in "${NODES[@]}"; do
    arch=$(linux_arch "$node")
    echo "$arch" > "$STATE_DIR/arch.$node"
    case " $archs " in *" $arch "*) ;; *) archs="$archs $arch" ;; esac
  done
  for arch in $archs; do
    log "Building zattera for linux/$arch"
    (cd "$REPO_DIR" && CGO_ENABLED=0 GOOS=linux GOARCH="$arch" \
      go build -trimpath -o "$STATE_DIR/zattera-linux-$arch" ./cmd/zattera)
  done
  log "Building local CLI ($ZT)"
  (cd "$REPO_DIR" && go build -o "$ZT" ./cmd/zattera)
}

# ------------------------------------------------------------- stage: install
write_unit() {
  local node="$1"
  zssh "$node" 'cat > /etc/systemd/system/zattera.service <<UNIT
[Unit]
Description=Zattera node
After=network-online.target docker.service
Wants=network-online.target
Requires=docker.service

[Service]
ExecStart=/usr/local/bin/zattera server --config /etc/zattera/config.toml
Restart=always
RestartSec=3
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload'
}

stage_install() {
  local node arch
  for node in "${NODES[@]}"; do
    arch=$(cat "$STATE_DIR/arch.$node" 2>/dev/null || linux_arch "$node")
    log "[$node] installing binary (linux/$arch) + unit"
    zssh "$node" 'systemctl stop zattera 2>/dev/null || true; mkdir -p /etc/zattera'
    zscp_to "$STATE_DIR/zattera-linux-$arch" "$node" /usr/local/bin/zattera
    zssh "$node" 'chmod +x /usr/local/bin/zattera'
    write_unit "$node"
  done

  log "[mario] writing control config"
  zssh mario "cat > /etc/zattera/config.toml <<CFG
node_name = \"mario\"
data_dir  = \"/var/lib/zattera\"
roles     = [\"control\", \"worker\"]
domain    = \"$DOMAIN_BASE\"

[api]
listen         = \":8443\"
advertise_addr = \"$API_HOST:8443\"

[registry]
listen = \":5000\"

[mesh]
listen_port      = 51820
public_endpoints = [\"$MARIO_IP:51820\"]

[acme]
email   = \"$ACME_EMAIL\"
staging = $ACME_STAGING
CFG"
}

# ------------------------------------------------------------- stage: control
stage_control() {
  log "[mario] starting control node"
  zssh mario 'systemctl enable --now zattera'
  wait_for 90 zssh mario 'ss -ltn | grep -q ":8443"' \
    || { zssh mario 'journalctl -u zattera --no-pager -n 50' >&2; die "API :8443 never came up on mario"; }

  # First-boot secrets land once in the journal; persist them locally.
  if [[ ! -f "$BOOTSTRAP_ENV" ]]; then
    log "[mario] capturing bootstrap token + CA fingerprint"
    local out token fp
    out=$(zssh mario 'journalctl -u zattera --no-pager')
    token=$(echo "$out" | grep -o 'zpat_[A-Za-z0-9_-]*' | head -1 || true)
    fp=$(echo "$out" | grep -o 'sha256=[0-9a-f]*' | head -1 | cut -d= -f2 || true)
    [[ -n "$fp" ]] || die "CA fingerprint not found in mario's journal"
    if [[ -z "$token" ]]; then
      die "bootstrap token not found in journal (data dir predates this run?). Run ./teardown.sh first for a fresh cluster."
    fi
    printf 'BOOT_TOKEN=%s\nCA_FINGERPRINT=%s\n' "$token" "$fp" > "$BOOTSTRAP_ENV"
  fi
  log "[mario] control node up"
}

# --------------------------------------------------------------- stage: login
stage_login() {
  source "$BOOTSTRAP_ENV"
  log "Logging in CLI context '$CLI_CONTEXT' → https://$API_HOST:8443"
  # Once the API holds its public ACME cert, system roots verify it and
  # --ca-pin can no longer match; before that (first seconds of a fresh
  # cluster) only the pin works. Try roots first, fall back to the pin.
  "$ZT" login --server "https://$API_HOST:8443" --token "$BOOT_TOKEN" --context "$CLI_CONTEXT" \
    || "$ZT" login --server "https://$API_HOST:8443" --ca-pin "$CA_FINGERPRINT" \
         --token "$BOOT_TOKEN" --context "$CLI_CONTEXT"
}

# ------------------------------------------------------------- stage: workers
stage_workers() {
  if [[ ! -s "$JOIN_TOKEN_FILE" ]]; then
    log "Creating reusable worker join token"
    # Reusable: workers re-run Join on every restart (pre-M2 limitation).
    "$ZT" nodes join-token create --single-use=false --json \
      | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])' > "$JOIN_TOKEN_FILE"
  fi
  local token node ip
  token=$(cat "$JOIN_TOKEN_FILE")

  for node in luigi peach; do
    ip=$(node_ip "$node")
    log "[$node] writing worker config + starting"
    zssh "$node" "cat > /etc/zattera/config.toml <<CFG
node_name = \"$node\"
data_dir  = \"/var/lib/zattera\"
roles     = [\"worker\"]

[join]
# IP on purpose: with the DNS name as SNI the API serves its public ACME
# chain (T-90) and the join token's cluster-CA pin cannot match; with no
# SNI (IP) the API serves the cluster-CA cert. See api/server.go.
addr  = \"$MARIO_IP:8443\"
token = \"$token\"

[mesh]
listen_port      = 51820
public_endpoints = [\"$ip:51820\"]
CFG"
    zssh "$node" 'systemctl reset-failed zattera 2>/dev/null || true
                  systemctl enable zattera; systemctl restart zattera'
  done

  log "Waiting for workers to appear in 'zt nodes ls'"
  wait_for 120 sh -c "'$ZT' nodes ls 2>/dev/null | grep -q luigi && '$ZT' nodes ls 2>/dev/null | grep -q peach" \
    || die "workers did not register"
  "$ZT" nodes ls
}

# ------------------------------------------------------------- stage: catrust
stage_catrust() {
  log "Distributing cluster CA to Docker trust stores ($REGISTRY_ADDR)"
  zssh mario 'cat /var/lib/zattera/ca/ca.crt' > "$CLUSTER_CA"
  local node
  for node in "${NODES[@]}"; do
    zssh "$node" "mkdir -p '/etc/docker/certs.d/$REGISTRY_ADDR'"
    zscp_to "$CLUSTER_CA" "$node" "/etc/docker/certs.d/$REGISTRY_ADDR/ca.crt"
  done
}

# -------------------------------------------------------------- stage: deploy
stage_deploy() {
  log "Creating project '$PROJECT' (ok if it exists)"
  "$ZT" projects create "$PROJECT" 2>/dev/null || true

  log "Deploying go-hello → production (first build is slow: cold BuildKit)"
  (cd "$REPO_DIR/test/fixtures/apps/go-hello" && "$ZT" deploy --prod --project "$PROJECT")

  log "Attaching custom domain $APP_DOMAIN"
  local out
  if ! out=$("$ZT" domains add "$APP_DOMAIN" --prod --app hello --project "$PROJECT" 2>&1); then
    if echo "$out" | grep -q "already in use"; then
      log "domain already attached"
    else
      die "domains add: $out"
    fi
  fi
  "$ZT" domains ls --project "$PROJECT"
}

# -------------------------------------------------------------- stage: verify
stage_verify() {
  log "Verifying https://$APP_DOMAIN/ (first hit triggers ACME issuance)"
  local i body
  for i in $(seq 1 20); do
    if body=$(curl -fsS --max-time 30 "https://$APP_DOMAIN/" 2>/dev/null); then
      echo "response: $body"
      echo | openssl s_client -connect "$APP_DOMAIN:443" -servername "$APP_DOMAIN" 2>/dev/null \
        | openssl x509 -noout -issuer -subject -dates
      log "OK → https://$APP_DOMAIN/"
      return 0
    fi
    sleep 10
  done
  die "https://$APP_DOMAIN/ not serving after ~3min; check: zssh mario journalctl -u zattera"
}

# ---------------------------------------------------------------------- main
ALL_STAGES=(keys docker build install control login workers catrust deploy verify)
if [[ $# -gt 0 ]]; then STAGES=("$@"); else STAGES=("${ALL_STAGES[@]}"); fi
for s in "${STAGES[@]}"; do
  log "===== stage: $s ====="
  "stage_$s"
done
log "Done. App URL: https://$APP_DOMAIN/  — CLI: $ZT (context '$CLI_CONTEXT', project '$PROJECT')"
