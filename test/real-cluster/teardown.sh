#!/usr/bin/env bash
# Tear the real test cluster down to bare hosts (Docker stays installed).
# Safe to re-run. After this, ./setup.sh rebuilds everything from scratch.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

ensure_ssh_access

# Workers first, control last.
for node in luigi peach mario; do
  log "[$node] stopping zattera + cleaning up"
  zssh "$node" "
    systemctl disable --now zattera 2>/dev/null || true
    rm -f /etc/systemd/system/zattera.service
    systemctl daemon-reload
    if command -v docker >/dev/null 2>&1; then
      docker ps -aq --filter label=dev.zattera/managed=true | xargs -r docker rm -f
      docker rm -f zt-system-buildkitd 2>/dev/null || true
      docker network ls --filter name=zt- -q | xargs -r docker network rm 2>/dev/null || true
      docker volume rm zt-buildkit-cache 2>/dev/null || true
    fi
    rm -rf /var/lib/zattera /etc/zattera '/etc/docker/certs.d/$REGISTRY_ADDR' /usr/local/bin/zattera
  " || warn "[$node] cleanup had errors (host unreachable?)"
done

# Local state tied to the destroyed cluster (SSH key + known_hosts are kept).
rm -f "$STATE_DIR/bootstrap.env" "$STATE_DIR/join.token" "$STATE_DIR/cluster-ca.crt"

log "Teardown complete. The '$CLI_CONTEXT' CLI context now points at a dead cluster;"
log "the next ./setup.sh run overwrites it."
