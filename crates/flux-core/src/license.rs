//! Offline license verification (Ed25519, ring).
//!
//! License key format: `base64url(json_payload) + "." + base64url(signature)`.
//! The issuing private key never ships with the product. Verification is fully
//! offline; no network calls, no telemetry.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use ring::signature::{UnparsedPublicKey, ED25519};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Fluxload licensing public key (Ed25519, 32 bytes, hex-encoded).
/// The matching private key is held offline by the issuer.
pub const LICENSE_PUBLIC_KEY_HEX: &str =
    "a4420f1f488a6d68340ff45695e9c50b016af4ed1b7461ff4d53b9af178103ab";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LicenseInfo {
    /// Product identifier, must be "fluxload".
    pub p: String,
    /// Licensed user / organization.
    pub u: String,
    /// Expiry, unix seconds (optional = perpetual).
    pub e: Option<u64>,
    /// Enabled feature flags (e.g. "pro").
    #[serde(default)]
    pub f: Vec<String>,
}

impl LicenseInfo {
    pub fn is_expired(&self, now_unix: u64) -> bool {
        matches!(self.e, Some(exp) if exp < now_unix)
    }

    pub fn has_feature(&self, feature: &str) -> bool {
        self.f.iter().any(|x| x == feature)
    }
}

fn public_key() -> [u8; 32] {
    let bytes =
        hex::decode(LICENSE_PUBLIC_KEY_HEX).expect("embedded license public key is valid hex");
    bytes.try_into().expect("32 bytes")
}

/// Verify a license key offline. Returns the decoded license on success.
pub fn verify_license_key(key: &str) -> Result<LicenseInfo, String> {
    let key = key.trim();
    let (payload_b64, sig_b64) = key
        .split_once('.')
        .ok_or_else(|| "malformed license key".to_string())?;
    let payload = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|e| format!("malformed license payload: {e}"))?;
    let sig = URL_SAFE_NO_PAD
        .decode(sig_b64)
        .map_err(|e| format!("malformed license signature: {e}"))?;
    if sig.len() != 64 {
        return Err("invalid license signature length".into());
    }
    let vk: UnparsedPublicKey<[u8; 32]> = UnparsedPublicKey::new(&ED25519, public_key());
    vk.verify(&payload, &sig)
        .map_err(|_| "license signature verification failed".to_string())?;
    let info: LicenseInfo =
        serde_json::from_slice(&payload).map_err(|e| format!("malformed license payload: {e}"))?;
    if info.p != crate::PRODUCT {
        return Err(format!("license is not for {}", crate::PRODUCT));
    }
    if info.is_expired(chrono::Utc::now().timestamp().max(0) as u64) {
        return Err("license expired".into());
    }
    Ok(info)
}

/// Load and verify a license from `<data_dir>/license.key`, if present.
pub fn load_license(data_dir: &Path) -> Option<LicenseInfo> {
    let path = data_dir.join("license.key");
    let key = std::fs::read_to_string(path).ok()?;
    match verify_license_key(&key) {
        Ok(info) => Some(info),
        Err(e) => {
            tracing::warn!("stored license rejected: {e}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A real key signed with the offline private key during the release pipeline.
    const VALID_DEMO_KEY: &str = "eyJwIjoiZmx1eGxvYWQiLCJ1IjoiRmx1eGxvYWQgVmFsaWRhdGlvbiBEZW1vIiwiZSI6NDEwMjQ0NDgwMCAsImYiOlsicHJvIl19.RLonsFOMxIgJ_3lwM_fNAoVChWk0gt0hlE80ciYvdjs7uhzfrefjxztlpt1EEJe55ig3NLqihTB9KXHLG6ORDA";

    #[test]
    fn verifies_signed_license() {
        let info = verify_license_key(VALID_DEMO_KEY).expect("demo license must verify");
        assert_eq!(info.p, "fluxload");
        assert_eq!(info.u, "Fluxload Validation Demo");
        assert!(info.has_feature("pro"));
    }

    #[test]
    fn rejects_tampered_license() {
        let (payload, sig) = VALID_DEMO_KEY.split_once('.').unwrap();
        // Tamper: modify one character of the payload.
        let tampered: String = payload
            .chars()
            .take(10)
            .chain("X".chars())
            .chain(payload.chars().skip(11))
            .collect();
        let forged = format!("{tampered}.{sig}");
        assert!(verify_license_key(&forged).is_err());
    }

    #[test]
    fn rejects_garbage() {
        assert!(verify_license_key("nonsense").is_err());
        assert!(verify_license_key("aaaa.bbbb").is_err());
    }
}
