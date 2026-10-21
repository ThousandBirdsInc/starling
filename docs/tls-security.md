# TLS dependency maintenance

## October 2026 eMudhra intermediate compromise

The lockfile uses `rustls-webpki` 0.103.16, whose verifier rejects certificate
paths containing the distrusted intermediate public keys, including alternate
cross-signs. The required `aws-lc-rs` / `aws-lc-sys` updates are included.

- [Upstream verifier release](https://github.com/rustls/webpki/releases/tag/v/0.103.16)
- [Mozilla incident report](https://bugzilla.mozilla.org/show_bug.cgi?id=2083449)
- [Trust-store operator guidance](https://groups.google.com/a/mozilla.org/g/dev-security-policy/c/0R3XLJL6FuE)

### Scope

The in-process Kubernetes transport (`STARLING_KUBE_RS=1`) uses kube-rs with
Rustls. Its trust anchors still come from the selected kubeconfig or in-cluster
configuration; this update does not replace cluster/private CAs with public
roots. A cluster using its own unaffected CA is not made vulnerable simply by
having this dependency in the lockfile. Never use `insecure-skip-tls-verify` to
work around a validation failure.

The default Kubernetes transport invokes **external `kubectl`**. Updating this
Rust dependency does not update kubectl, Docker, credential plugins, the OS
trust store, or any other external TLS client. Update those independently using
their vendor's incident guidance and verify the clients actually deployed.

Starling's locally generated HTTPS proxy CA is unrelated to eMudhra. This
incident does not require rotating it or changing the machine's trust settings.

### Rollout and verification

1. Build and test from the updated lockfile with `cargo test --locked` and
   `cargo build --release --locked`. Rebuild/reinstall deployed binaries and
   restart running Starling processes; changing the repository alone does not
   patch already compiled software.
2. Confirm `cargo tree --locked -i rustls-webpki` resolves to 0.103.16 or a later
   patched compatible release.
3. Run the existing kind/Docker integration suite on its disposable test
   cluster. Check both the default kubectl transport and `STARLING_KUBE_RS=1`
   with the intended kubeconfig, including private-CA authentication.
4. Where a deployed client trusts public CAs, use the
   [incident test hosts](https://emudhra.we-have-a-problem.com/) to check both
   direct eMudhra and AAA cross-signed chains with normal verification enabled.
   A DNS error, timeout, or interception by a proxy is not proof of certificate
   rejection; also verify an ordinary valid endpoint remains reachable.

Do not remove entire root stores or rotate unrelated private CAs as a shortcut.
