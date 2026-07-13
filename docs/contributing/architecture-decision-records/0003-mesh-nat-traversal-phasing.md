# ADR-0003 — WireGuard mesh: phased NAT traversal (hub → direct → punch → relay)

**Status:** Accepted · **Date:** 2026-07-13

## Context

F1/§3.1 promise cross-cloud and NAT'd home servers with only outbound UDP. A full magicsock-style
implementation (custom bind, hole punching, relays) is the riskiest component in the project.
Building it monolithically would block everything else.

## Decision

Four incremental phases, each shippable and each keeping the previous as fallback:

- **Phase A — hub-and-spoke (M1):** workers peer only with control nodes; `AllowedIPs 10.90.0.0/16`
  routes via the control peer; control nodes enable IP forwarding and relay worker↔worker traffic at the
  IP layer. NAT'd workers work day one (outbound UDP + `PersistentKeepalive=25`).
- **Phase B — direct peering (M1):** control nodes observe each node's public `addr:port` via a
  disco/STUN-lite echo (3-message protocol, node-key HMAC) and distribute full peer sets; mutually
  reachable nodes peer directly with narrowed per-peer AllowedIPs. Hub route remains the fallback
  (WG picks the most-specific AllowedIP).
- **Phase C — meshsock (M2):** custom `conn.Bind` for wireguard-go multiplexing WG + disco packets on one
  UDP socket; control-plane-signaled simultaneous-open hole punching; per-peer path state machine
  (direct → punched → relay).
- **Phase D — TCP relay, DERP-lite (M2):** every control node runs an mTLS TCP relay framing
  `(dst node, WG payload)`; meshsock falls back to it when no UDP path works.

Kernel WireGuard is used when available in phases A/B only; nodes needing punching/relay stay on
wireguard-go (kernel WG cannot use a custom bind). Single-node mode disables the mesh entirely.

## Consequences

- M1 ships with working NAT support (slower path through control nodes) without any magicsock risk.
- memberlist gossip arrives with M2 (it needs the mesh; heartbeats suffice for a single control node).
- The `mesh.Manager` interface hides all of this from the rest of the codebase.
