//go:build e2e

// Package e2e holds the full single-node smoke test (`make test-e2e`,
// task T-54): build the binary, boot `zattera server --dev`, deploy the
// go-hello fixture through the whole pipeline, assert the URL serves, test
// red/green + rollback, run a one-shot job, then tear down and assert no
// managed containers survive.
//
// # Requirements
//
//   - A reachable Docker daemon (the test skips when `docker version` fails).
//
//   - Outbound DNS for sslip.io. `hello-production.apps.127.0.0.1.sslip.io`
//     resolves to 127.0.0.1 in CI. If your local resolver rewrites or blocks
//     sslip.io, add a fallback to /etc/hosts:
//
//     127.0.0.1 hello-production.apps.127.0.0.1.sslip.io
//
//     The HTTP assertions also send an explicit Host header, so name-based
//     routing works even when only the numeric address resolves.
//
//   - Designed for a Linux CI runner (or a privileged container locally); the
//     ingress/mesh data path assumes Linux container networking.
//
// Every wait in the harness is a bounded poll (the first deploy allows ~180s
// for a cold BuildKit start plus the fixture image build).
package e2e
