# Shared helpers for the real-cluster scripts. Source, don't execute.
# Requires bash 3.2+ (macOS default), ssh, scp, expect, go.

set -euo pipefail

RC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$RC_DIR/../.." && pwd)"
STATE_DIR="$RC_DIR/.state"
mkdir -p "$STATE_DIR"

# shellcheck source=servers.env
source "$RC_DIR/servers.env"

SSH_KEY="$STATE_DIR/id_ed25519"
SSH_OPTS=(-i "$SSH_KEY" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new
          -o "UserKnownHostsFile=$STATE_DIR/known_hosts" -o ConnectTimeout=10)

ZT="$REPO_DIR/zt"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[!]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[x]\033[0m %s\n' "$*" >&2; exit 1; }

# node_ip mario → $MARIO_IP (bash-3.2-safe indirection)
node_var() { echo "$(echo "$1" | tr '[:lower:]' '[:upper:]')_$2"; }
node_ip()  { local v; v=$(node_var "$1" IP); eval "echo \"\$$v\""; }

# Current valid password: the rotated one (.state/pw.<node>) wins over
# servers.env (the hosts force a password change on first login).
node_pw()  {
  if [[ -f "$STATE_DIR/pw.$1" ]]; then cat "$STATE_DIR/pw.$1"; return; fi
  local v; v=$(node_var "$1" PW); eval "echo \"\$$v\""
}

# zssh <node> <command...> — run a command on a node over the key.
zssh() {
  local node="$1"; shift
  ssh "${SSH_OPTS[@]}" "$SSH_USER@$(node_ip "$node")" "$@"
}

# zscp <local-path> <node>:<remote-path>  (one direction, one file)
zscp_to() {
  local src="$1" node="$2" dst="$3"
  scp -q "${SSH_OPTS[@]}" "$src" "$SSH_USER@$(node_ip "$node"):$dst"
}

can_ssh() {
  ssh "${SSH_OPTS[@]}" -o BatchMode=yes "$SSH_USER@$(node_ip "$1")" true 2>/dev/null
}

ensure_ssh_key() {
  if [[ ! -f "$SSH_KEY" ]]; then
    log "Generating dedicated SSH key $SSH_KEY"
    ssh-keygen -q -t ed25519 -N '' -f "$SSH_KEY" -C zattera-real-cluster
  fi
}

# fix_expired_password <node> — the test hosts force a password change on the
# first login. Complete it over an ssh TTY, rotating to a generated password
# kept in .state/pw.<node>. No-op when login already works or key auth is up.
fix_expired_password() {
  local node="$1" ip pw newpw rc
  ip=$(node_ip "$node"); pw=$(node_pw "$node")
  if [[ ! -f "$STATE_DIR/pw.$node" ]]; then
    newpw="Zt-$(openssl rand -hex 12)"
  else
    newpw=$(cat "$STATE_DIR/pw.$node")
  fi
  set +e
  expect -f - "$ip" "$pw" "$newpw" <<'EXPECT_EOF'
set ip    [lindex $argv 0]
set pw    [lindex $argv 1]
set newpw [lindex $argv 2]
set timeout 45
spawn ssh -tt -o StrictHostKeyChecking=accept-new -o "UserKnownHostsFile=$env(RC_KNOWN_HOSTS)" \
    -o PreferredAuthentications=keyboard-interactive,password -o PubkeyAuthentication=no \
    root@$ip echo ZT_LOGIN_OK
set changed 0
expect {
    -re "(?i)(current|old).*password.*:"   { send "$pw\r";    exp_continue }
    -re "(?i)(new|retype|re-enter).*password.*:" { set changed 1; send "$newpw\r"; exp_continue }
    -re "(?i)password.*:"                  { send "$pw\r";    exp_continue }
    "ZT_LOGIN_OK"                          { exp_continue }
    -re "(?i)permission denied"            { exit 3 }
    timeout                                { exit 4 }
    eof {}
}
catch wait result
if {$changed} { exit 2 }
exit [lindex $result 3]
EXPECT_EOF
  rc=$?
  set -e
  case "$rc" in
    0) ;;
    2) log "[$node] expired password rotated"
       printf '%s' "$newpw" > "$STATE_DIR/pw.$node"; chmod 600 "$STATE_DIR/pw.$node" ;;
    3) die "[$node] password rejected — update servers.env (or remove stale .state/pw.$node)" ;;
    *) die "[$node] could not complete first login (rc=$rc)" ;;
  esac
}

# install_key <node> — push the key using the currently valid password (expect).
install_key() {
  local node="$1" ip pw
  fix_expired_password "$node"
  ip=$(node_ip "$node"); pw=$(node_pw "$node")
  log "Installing SSH key on $node ($ip) via password"
  expect -f - "$ip" "$pw" <<'EXPECT_EOF'
set ip [lindex $argv 0]
set pw [lindex $argv 1]
set timeout 40
spawn ssh-copy-id -i $env(RC_SSH_PUB) -o StrictHostKeyChecking=accept-new -o "UserKnownHostsFile=$env(RC_KNOWN_HOSTS)" root@$ip
expect {
    -re "(?i)password.*:" { send "$pw\r"; exp_continue }
    "ERROR"               { exit 1 }
    eof
}
catch wait result
exit [lindex $result 3]
EXPECT_EOF
}

ensure_ssh_access() {
  ensure_ssh_key
  export RC_SSH_PUB="$SSH_KEY.pub" RC_KNOWN_HOSTS="$STATE_DIR/known_hosts"
  local node
  for node in "${NODES[@]}"; do
    if can_ssh "$node"; then
      log "SSH ok: $node"
    else
      install_key "$node"
      can_ssh "$node" || die "still cannot SSH into $node after key install"
    fi
  done
}

# linux_arch <node> → amd64 | arm64
linux_arch() {
  case "$(zssh "$1" uname -m)" in
    x86_64)  echo amd64 ;;
    aarch64) echo arm64 ;;
    *) die "unsupported arch on $1" ;;
  esac
}

# Wait until <cmd...> succeeds, up to $1 seconds.
wait_for() {
  local timeout="$1" start now; shift
  start=$(date +%s)
  until "$@" 2>/dev/null; do
    now=$(date +%s)
    (( now - start > timeout )) && return 1
    sleep 3
  done
}
