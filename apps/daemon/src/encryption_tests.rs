//! Advanced Encryption Tests (Days 13-14)
//! Comprehensive test suite for encryption at-rest functionality

#[cfg(test)]
mod encryption_at_rest_tests {
    use adm_domain::encryption_at_rest::*;
    use adm_domain::key_manager::*;
    use tempfile::TempDir;

    #[test]
    fn test_cipher_suite_variants() {
        assert_eq!(EncryptionAlgorithm::Aes256Gcm.as_str(), "AES-256-GCM");
        assert_eq!(
            EncryptionAlgorithm::ChaCha20Poly1305.as_str(),
            "ChaCha20-Poly1305"
        );
    }

    #[test]
    fn test_pbkdf2_secure_params() {
        let params = KeyDerivationParams::secure_pbkdf2();
        assert!(params.validate().is_ok());
        assert_eq!(params.iterations, 600_000);
        assert_eq!(params.output_length, 32);
    }

    #[test]
    fn test_pbkdf2_iterations_validation() {
        let mut params = KeyDerivationParams::secure_pbkdf2();
        params.iterations = 50_000;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_pbkdf2_salt_size_validation() {
        let mut params = KeyDerivationParams::secure_pbkdf2();
        params.salt_size = 8;
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_encryption_key_creation() {
        let key = EncryptionKey::new("test-key-1", EncryptionAlgorithm::Aes256Gcm);
        assert_eq!(key.key_id, "test-key-1");
        assert!(key.is_active);
        assert!(!key.is_expired());
    }

    #[test]
    fn test_encryption_key_expiration() {
        let mut key = EncryptionKey::new("test-key-1", EncryptionAlgorithm::Aes256Gcm);
        key.expires_at = 0;
        assert!(key.is_expired());
    }

    #[test]
    fn test_database_encryption_default() {
        let config = DatabaseEncryptionConfig::default();
        assert!(!config.enabled);
        assert!(!config.encrypt_database_file);
    }

    #[test]
    fn test_database_encryption_production() {
        let config = DatabaseEncryptionConfig::production();
        assert!(config.enabled);
        assert!(config.encrypt_database_file);
        assert!(config.encrypt_wal);
        assert!(!config.encrypted_fields.is_empty());
    }

    #[test]
    fn test_database_encryption_validation() {
        let config = DatabaseEncryptionConfig::production();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_key_rotation_policy_default() {
        let policy = KeyRotationPolicy::default();
        assert!(policy.enabled);
        assert_eq!(policy.rotation_interval_days, 365);
    }

    #[test]
    fn test_key_rotation_policy_aggressive() {
        let policy = KeyRotationPolicy::aggressive();
        assert!(policy.enabled);
        assert_eq!(policy.rotation_interval_days, 90);
        assert!(policy.re_encrypt_after_rotation);
    }

    #[test]
    fn test_classification_encryption_requirement() {
        assert!(!Classification::Public.requires_encryption());
        assert!(Classification::Internal.requires_encryption());
        assert!(Classification::Confidential.requires_encryption());
        assert!(Classification::Restricted.requires_encryption());
    }

    #[test]
    fn test_encrypted_data_validation() {
        let encrypted = EncryptedData {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_id: "key-1".to_string(),
            iv: "iv_value_at_least_16_chars".to_string(),
            ciphertext: "ciphertext".to_string(),
            tag: "tag_value_at_least_16".to_string(),
            aad: None,
            encrypted_at: 0,
        };

        assert!(encrypted.validate().is_ok());
    }

    #[test]
    fn test_encryption_service_key_generation() {
        let params = KeyDerivationParams::secure_pbkdf2();
        let salt = vec![0u8; 32];

        let result = EncryptionService::derive_key("password", &salt, &params);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 32);
    }

    #[test]
    fn test_key_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let policy = KeyRotationPolicy::default();
        let manager = KeyManager::new(temp_dir.path().to_path_buf(), KeyStorageType::File, policy);

        assert!(manager.is_ok());
    }

    #[test]
    fn test_key_manager_key_creation() {
        let temp_dir = TempDir::new().unwrap();
        let mut manager = KeyManager::new(
            temp_dir.path().to_path_buf(),
            KeyStorageType::File,
            KeyRotationPolicy::default(),
        )
        .unwrap();

        let key_id = manager.create_key(EncryptionAlgorithm::Aes256Gcm);
        assert!(key_id.is_ok());
    }

    #[test]
    fn test_key_manager_activation() {
        let temp_dir = TempDir::new().unwrap();
        let mut manager = KeyManager::new(
            temp_dir.path().to_path_buf(),
            KeyStorageType::File,
            KeyRotationPolicy::default(),
        )
        .unwrap();

        let key_id = manager.create_key(EncryptionAlgorithm::Aes256Gcm).unwrap();
        manager.activate_key(&key_id).unwrap();

        assert_eq!(manager.get_active_key_id().unwrap(), key_id);
    }

    #[test]
    fn test_key_manager_rotation() {
        let temp_dir = TempDir::new().unwrap();
        let mut manager = KeyManager::new(
            temp_dir.path().to_path_buf(),
            KeyStorageType::File,
            KeyRotationPolicy::default(),
        )
        .unwrap();

        let key1 = manager.create_key(EncryptionAlgorithm::Aes256Gcm).unwrap();
        manager.activate_key(&key1).unwrap();

        let key2 = manager.rotate_key(&key1).unwrap();
        assert!(manager.get_active_key_id().unwrap() != key1);
        assert_eq!(manager.get_active_key_id().unwrap(), key2);
    }

    #[test]
    fn test_key_manager_statistics() {
        let temp_dir = TempDir::new().unwrap();
        let mut manager = KeyManager::new(
            temp_dir.path().to_path_buf(),
            KeyStorageType::File,
            KeyRotationPolicy::default(),
        )
        .unwrap();

        let key_id = manager.create_key(EncryptionAlgorithm::Aes256Gcm).unwrap();
        manager.activate_key(&key_id).unwrap();

        let stats = manager.get_statistics();
        assert!(stats.active_keys > 0);
    }

    #[test]
    fn test_key_compromise_detection() {
        let temp_dir = TempDir::new().unwrap();
        let mut manager = KeyManager::new(
            temp_dir.path().to_path_buf(),
            KeyStorageType::File,
            KeyRotationPolicy::default(),
        )
        .unwrap();

        let key_id = manager.create_key(EncryptionAlgorithm::Aes256Gcm).unwrap();
        manager.activate_key(&key_id).unwrap();
        manager.mark_compromised(&key_id).unwrap();

        let metadata = manager.get_key_metadata(&key_id).unwrap();
        assert_eq!(metadata.status, KeyStatus::Compromised);
    }

    #[test]
    fn test_credential_vault_operations() {
        use adm_domain::encrypted_storage::*;

        let mut vault = CredentialVault::new();

        let encrypted = EncryptedData {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_id: "key-1".to_string(),
            iv: "iv".to_string(),
            ciphertext: "ciphertext".to_string(),
            tag: "tag_value_here_16chars".to_string(),
            aad: None,
            encrypted_at: 0,
        };

        let field = EncryptedField::new("api_key", encrypted, "hash".to_string());
        vault.store_credential("cred-1", field).unwrap();

        let retrieved = vault.retrieve_credential("cred-1").unwrap();
        assert_eq!(retrieved.name, "api_key");
    }

    #[test]
    fn test_credential_vault_access_control() {
        use adm_domain::encrypted_storage::*;

        let mut vault = CredentialVault::new();
        vault.grant_access("user-1", "cred-1");

        assert!(vault.can_access("user-1", "cred-1"));
        assert!(!vault.can_access("user-2", "cred-1"));
    }

    #[test]
    fn test_masking_strategies() {
        use adm_domain::encrypted_storage::*;

        let strategy = MaskingStrategy::FirstLastFour;
        let result = strategy.apply("12345678");
        assert!(result.contains("1234") && result.contains("5678"));

        let strategy = MaskingStrategy::Complete;
        let result = strategy.apply("secret");
        assert_eq!(result, "****");
    }

    #[test]
    fn test_key_storage_types() {
        assert_eq!(KeyStorageType::File.as_str(), "file");
        assert_eq!(KeyStorageType::Hsm.as_str(), "hsm");
        assert_eq!(KeyStorageType::Kms.as_str(), "kms");
        assert_eq!(KeyStorageType::Environment.as_str(), "environment");
    }

    #[test]
    fn test_key_status_transitions() {
        assert_eq!(KeyStatus::Created.as_str(), "created");
        assert_eq!(KeyStatus::Active.as_str(), "active");
        assert_eq!(KeyStatus::Archived.as_str(), "archived");
        assert_eq!(KeyStatus::Compromised.as_str(), "compromised");
    }

    #[test]
    fn test_encrypted_file_storage() {
        use adm_domain::encrypted_storage::*;

        let temp_dir = TempDir::new().unwrap();
        let storage = EncryptedFileStorage::new(
            temp_dir.path().to_path_buf(),
            EncryptionAlgorithm::Aes256Gcm,
        );

        assert!(storage.initialize().is_ok());
    }

    #[test]
    fn test_multi_level_key_rotation_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let policy = KeyRotationPolicy::default();
        let mut manager =
            KeyManager::new(temp_dir.path().to_path_buf(), KeyStorageType::File, policy).unwrap();

        let key1 = manager.create_key(EncryptionAlgorithm::Aes256Gcm).unwrap();
        manager.activate_key(&key1).unwrap();

        let key2 = manager.rotate_key(&key1).unwrap();
        let key3 = manager.rotate_key(&key2).unwrap();

        assert_ne!(key1, key2);
        assert_ne!(key2, key3);
        assert_eq!(manager.get_active_key_id().unwrap(), key3);
    }

    #[test]
    fn test_key_expiration_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let mut manager = KeyManager::new(
            temp_dir.path().to_path_buf(),
            KeyStorageType::File,
            KeyRotationPolicy::default(),
        )
        .unwrap();

        let key_id = manager.create_key(EncryptionAlgorithm::Aes256Gcm).unwrap();

        // Should not need rotation immediately after creation
        let needs_rotation = manager.needs_rotation(&key_id).unwrap();
        assert!(!needs_rotation);
    }

    #[test]
    fn test_database_encryption_field_configuration() {
        let config = DatabaseEncryptionConfig::production();
        assert!(config.encrypted_fields.contains(&"password".to_string()));
        assert!(config.encrypted_fields.contains(&"api_key".to_string()));
    }

    #[test]
    fn test_encryption_algorithm_selection() {
        let algo1 = EncryptionAlgorithm::Aes256Gcm;
        let algo2 = EncryptionAlgorithm::ChaCha20Poly1305;

        assert_ne!(algo1, algo2);
        assert_eq!(algo1.as_str(), "AES-256-GCM");
    }
}
