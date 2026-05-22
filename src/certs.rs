//! Local CA + leaf certificate generation for the HTTPS proxy.
//!
//! Ported from portless's TLS story: on first use we generate a local CA and a
//! wildcard leaf for `*.<tld>`, persisted under `~/.starling/`. `starling trust`
//! installs the CA into the system trust store so browsers accept the leaf.

use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose};

use crate::daemon::protocol::state_dir;

pub fn ca_cert_path() -> PathBuf {
    state_dir().join("ca.pem")
}
pub fn ca_key_path() -> PathBuf {
    state_dir().join("ca-key.pem")
}
/// Ensure the CA exists, returning its (cert_pem, key_pem). Generated once.
pub fn ensure_ca() -> Result<(String, String)> {
    let cert_path = ca_cert_path();
    let key_path = ca_key_path();
    if cert_path.exists() && key_path.exists() {
        let cert = std::fs::read_to_string(&cert_path)?;
        let key = std::fs::read_to_string(&key_path)?;
        return Ok((cert, key));
    }
    std::fs::create_dir_all(state_dir()).ok();

    let mut params = CertificateParams::new(vec![])
        .context("creating CA params")?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    params
        .distinguished_name
        .push(DnType::CommonName, "Starling Local CA");
    params
        .distinguished_name
        .push(DnType::OrganizationName, "Starling");

    let key = KeyPair::generate().context("generating CA key")?;
    let cert = params.self_signed(&key).context("self-signing CA")?;
    let cert_pem = cert.pem();
    let key_pem = key.serialize_pem();
    std::fs::write(&cert_path, &cert_pem)?;
    std::fs::write(&key_path, &key_pem)?;
    Ok((cert_pem, key_pem))
}

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio_rustls::rustls::crypto::aws_lc_rs::sign::any_supported_type;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::{ClientHello, ResolvesServerCert};
use tokio_rustls::rustls::sign::CertifiedKey;
use tokio_rustls::rustls::ServerConfig;

/// Generates per-hostname leaf certs on demand (via SNI), each signed by the
/// local CA, with the exact requested hostname as its SAN. Required for
/// `.localhost` names, where a `*.localhost` wildcard is rejected as a
/// TLD-level wildcard by browsers/curl.
struct HostCertResolver {
    ca_cert_pem: String,
    ca_key_pem: String,
    ca_der: CertificateDer<'static>,
    cache: Mutex<HashMap<String, Arc<CertifiedKey>>>,
}

impl std::fmt::Debug for HostCertResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HostCertResolver")
    }
}

impl HostCertResolver {
    fn make_cert(&self, name: &str) -> Result<CertifiedKey> {
        let ca_key = KeyPair::from_pem(&self.ca_key_pem)?;
        let ca_cert = CertificateParams::from_ca_cert_pem(&self.ca_cert_pem)?.self_signed(&ca_key)?;
        let leaf_key = KeyPair::generate()?;
        let mut params = CertificateParams::new(vec![name.to_string()])?;
        params.distinguished_name.push(DnType::CommonName, name);
        let leaf = params.signed_by(&leaf_key, &ca_cert, &ca_key)?;
        let leaf_der = leaf.der().clone();
        let key_der = PrivateKeyDer::try_from(leaf_key.serialize_der())
            .map_err(|e| anyhow!("leaf key: {e}"))?;
        let signing_key = any_supported_type(&key_der).map_err(|e| anyhow!("signing key: {e}"))?;
        Ok(CertifiedKey::new(
            vec![leaf_der, self.ca_der.clone()],
            signing_key,
        ))
    }
}

impl ResolvesServerCert for HostCertResolver {
    fn resolve(&self, hello: ClientHello) -> Option<Arc<CertifiedKey>> {
        let name = hello.server_name().unwrap_or("localhost").to_string();
        if let Some(ck) = self.cache.lock().unwrap().get(&name) {
            return Some(ck.clone());
        }
        let ck = Arc::new(self.make_cert(&name).ok()?);
        self.cache.lock().unwrap().insert(name, ck.clone());
        Some(ck)
    }
}

/// Build a rustls `ServerConfig` that mints a matching cert per SNI hostname.
pub fn tls_server_config() -> Result<ServerConfig> {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    let (ca_cert_pem, ca_key_pem) = ensure_ca()?;
    let ca_key = KeyPair::from_pem(&ca_key_pem)?;
    let ca_cert = CertificateParams::from_ca_cert_pem(&ca_cert_pem)?.self_signed(&ca_key)?;
    let ca_der = ca_cert.der().clone();

    let resolver = HostCertResolver {
        ca_cert_pem,
        ca_key_pem,
        ca_der,
        cache: Mutex::new(HashMap::new()),
    };
    Ok(ServerConfig::builder()
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(resolver)))
}

/// Install the CA into the OS trust store (best-effort, per-platform).
pub fn install_trust() -> Result<()> {
    let (_cert, _key) = ensure_ca()?;
    let ca = ca_cert_path();
    let ca_str = ca.to_string_lossy().to_string();

    #[cfg(target_os = "macos")]
    let status = std::process::Command::new("sudo")
        .args([
            "security",
            "add-trusted-cert",
            "-d",
            "-r",
            "trustRoot",
            "-k",
            "/Library/Keychains/System.keychain",
            &ca_str,
        ])
        .status();

    #[cfg(target_os = "linux")]
    let status = {
        // Debian/Ubuntu layout; other distros use update-ca-trust.
        let dest = "/usr/local/share/ca-certificates/starling-ca.crt";
        let _ = std::process::Command::new("sudo")
            .args(["cp", &ca_str, dest])
            .status();
        std::process::Command::new("sudo")
            .arg("update-ca-certificates")
            .status()
    };

    #[cfg(target_os = "windows")]
    let status = std::process::Command::new("certutil")
        .args(["-addstore", "-f", "ROOT", &ca_str])
        .status();

    match status {
        Ok(s) if s.success() => {
            println!("Installed Starling CA into the system trust store.");
            Ok(())
        }
        Ok(s) => Err(anyhow!("trust install command failed: {s}")),
        Err(e) => Err(anyhow!("running trust install command: {e}")),
    }
}
