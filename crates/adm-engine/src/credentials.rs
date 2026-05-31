//! Secure credential encryption and storage.
//!
//! Encrypts authentication credentials (usernames, passwords, tokens)
//! using AES-256-GCM with secure key derivation (PBKDF2).

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CredentialType {
    BasicAuth,
    BearerToken,
    ApiKey,
    CustomHeader,
    CookieJar,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credential {
    pub id: String,
    pub credential_type: CredentialType,
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
    pub key: Option<String>,
    pub value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedCredential {
    pub id: String,
    pub credential_type: String,
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
    pub salt: Vec<u8>,
    pub version: u32,
}

pub struct CredentialVault {
    master_password: String,
}

impl CredentialVault {
    #[must_use]
    pub const fn new(master_password: String) -> Self {
        Self { master_password }
    }

    /// Encrypt a credential for storage.
    ///
    /// # Errors
    ///
    /// Returns an error if credential serialization or AES-GCM encryption fails.
    pub fn encrypt(&self, credential: &Credential) -> Result<EncryptedCredential> {
        use aes_gcm::aead::Aead;
        use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
        use pbkdf2::pbkdf2_hmac;
        use rand::RngExt;
        use sha2::Sha256;

        let mut rng = rand::rng();

        // Generate random salt
        let mut salt = [0u8; 16];
        rng.fill(&mut salt);

        // Derive encryption key from master password + salt
        let mut key_bytes = [0u8; 32];
        pbkdf2_hmac::<Sha256>(
            self.master_password.as_bytes(),
            &salt,
            100_000,
            &mut key_bytes,
        );

        let key = Key::<Aes256Gcm>::from(key_bytes);
        let cipher = Aes256Gcm::new(&key);

        // Generate random nonce
        let mut nonce_bytes = [0u8; 12];
        rng.fill(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        // Serialize credential
        let plaintext = serde_json::to_vec(credential)?;

        // Encrypt
        let ciphertext = cipher
            .encrypt(nonce, plaintext.as_ref())
            .map_err(|_| anyhow::anyhow!("Encryption failed"))?;

        Ok(EncryptedCredential {
            id: credential.id.clone(),
            credential_type: format!("{:?}", credential.credential_type),
            ciphertext,
            nonce: nonce_bytes.to_vec(),
            salt: salt.to_vec(),
            version: 1,
        })
    }

    /// Decrypt a stored credential.
    ///
    /// # Errors
    ///
    /// Returns an error if the encrypted payload is malformed, the master
    /// password is incorrect, decryption fails, or deserialization fails.
    pub fn decrypt(&self, encrypted: &EncryptedCredential) -> Result<Credential> {
        use aes_gcm::aead::Aead;
        use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
        use pbkdf2::pbkdf2_hmac;
        use sha2::Sha256;

        // Validate salt and nonce length
        let salt: [u8; 16] = encrypted.salt[..16].try_into()?;
        let nonce_bytes: [u8; 12] = encrypted.nonce[..12].try_into()?;

        // Derive same key
        let mut key_bytes = [0u8; 32];
        pbkdf2_hmac::<Sha256>(
            self.master_password.as_bytes(),
            &salt,
            100_000,
            &mut key_bytes,
        );

        let key = Key::<Aes256Gcm>::from(key_bytes);
        let cipher = Aes256Gcm::new(&key);
        let nonce = Nonce::from_slice(&nonce_bytes);

        // Decrypt
        let plaintext = cipher
            .decrypt(nonce, encrypted.ciphertext.as_ref())
            .map_err(|_| anyhow::anyhow!("Decryption failed - master password may be incorrect"))?;

        // Deserialize
        let credential: Credential = serde_json::from_slice(&plaintext)?;

        Ok(credential)
    }
}

/// Load credentials from encrypted vault file.
///
/// # Errors
///
/// Returns an error if the vault file cannot be read or parsed.
pub async fn load_vault(vault_path: &Path, master_password: &str) -> Result<Vec<Credential>> {
    let content = tokio::fs::read_to_string(vault_path)
        .await
        .context("Failed to read vault file")?;

    let encrypted_creds: Vec<EncryptedCredential> = serde_json::from_str(&content)?;
    let vault = CredentialVault::new(master_password.to_string());

    let mut credentials = Vec::new();
    for encrypted in encrypted_creds {
        match vault.decrypt(&encrypted) {
            Ok(cred) => credentials.push(cred),
            Err(e) => {
                tracing::warn!(id = %encrypted.id, error = %e, "Failed to decrypt credential");
            }
        }
    }

    Ok(credentials)
}

/// Save credentials to encrypted vault file.
///
/// # Errors
///
/// Returns an error if any credential cannot be encrypted, the vault cannot be
/// serialized, or the destination file cannot be written.
pub async fn save_vault(
    vault_path: &Path,
    credentials: &[Credential],
    master_password: &str,
) -> Result<()> {
    let vault = CredentialVault::new(master_password.to_string());

    let mut encrypted_creds = Vec::new();
    for cred in credentials {
        encrypted_creds.push(vault.encrypt(cred)?);
    }

    let content = serde_json::to_string_pretty(&encrypted_creds)?;
    tokio::fs::write(vault_path, content).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credential_encryption_decryption() {
        let vault = CredentialVault::new("test_password".to_string());

        let cred = Credential {
            id: "test-cred".to_string(),
            credential_type: CredentialType::BasicAuth,
            username: Some("user".to_string()),
            password: Some("secret".to_string()),
            token: None,
            key: None,
            value: None,
        };

        let encrypted = vault.encrypt(&cred).unwrap();
        assert!(!encrypted.ciphertext.is_empty());

        let decrypted = vault.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted.id, cred.id);
        assert_eq!(decrypted.username, cred.username);
        assert_eq!(decrypted.password, cred.password);
    }

    #[test]
    fn test_wrong_password_fails() {
        let vault = CredentialVault::new("password1".to_string());

        let cred = Credential {
            id: "test".to_string(),
            credential_type: CredentialType::BearerToken,
            username: None,
            password: None,
            token: Some("token123".to_string()),
            key: None,
            value: None,
        };

        let encrypted = vault.encrypt(&cred).unwrap();

        // Try to decrypt with wrong password
        let wrong_vault = CredentialVault::new("wrong_password".to_string());
        let result = wrong_vault.decrypt(&encrypted);

        assert!(result.is_err());
    }
}
