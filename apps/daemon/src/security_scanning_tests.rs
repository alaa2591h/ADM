//! Security Scanning and SIEM Logging Tests (Days 15-16)

#[cfg(test)]
mod security_scanning_tests {
    use adm_domain::security_policy::*;
    use adm_domain::siem_logging::*;
    use adm_domain::vulnerability_scanner::*;

    // Vulnerability Scanner Tests
    #[test]
    fn test_cvss_level_calculation() {
        assert_eq!(CvssLevel::from_score(0.0), CvssLevel::None);
        assert_eq!(CvssLevel::from_score(2.5), CvssLevel::Low);
        assert_eq!(CvssLevel::from_score(5.0), CvssLevel::Medium);
        assert_eq!(CvssLevel::from_score(7.5), CvssLevel::High);
        assert_eq!(CvssLevel::from_score(9.5), CvssLevel::Critical);
    }

    #[test]
    fn test_cve_creation_and_validation() {
        let cve = Cve::new("CVE-2024-1234", "serde", 8.5, "Test vulnerability");
        assert_eq!(cve.id, "CVE-2024-1234");
        assert_eq!(cve.package, "serde");
        assert_eq!(cve.cvss_score, 8.5);
        assert_eq!(cve.cvss_level, CvssLevel::High);
        assert!(cve.validate().is_ok());
    }

    #[test]
    fn test_cve_critical_detection() {
        let mut cve = Cve::new("CVE-2024-9999", "pkg", 9.5, "Critical");
        assert!(cve.is_critical());

        cve.cvss_score = 3.0;
        cve.cvss_level = CvssLevel::from_score(3.0);
        assert!(!cve.is_critical());
    }

    #[test]
    fn test_vulnerability_status_tracking() {
        assert!(!VulnerabilityStatus::Resolved.is_critical());
        assert!(VulnerabilityStatus::Confirmed.is_critical());
        assert!(VulnerabilityStatus::Reported.is_critical());
    }

    #[test]
    fn test_package_scan_result() {
        let mut result = PackageScanResult::new("tokio", "1.0.0");

        let cve = Cve::new("CVE-2024-5555", "tokio", 7.0, "Async issue");
        result.add_vulnerability(cve).unwrap();

        assert_eq!(result.vulnerability_count, 1);
        assert!(result.has_critical());
        assert_eq!(result.max_cvss_score, 7.0);
    }

    #[test]
    fn test_vulnerability_scan_report() {
        let mut report = VulnerabilityScanReport::new();

        let mut pkg = PackageScanResult::new("pkg", "1.0");
        let cve = Cve::new("CVE-2024-1111", "pkg", 8.0, "Test");
        pkg.add_vulnerability(cve).unwrap();

        report.add_package_result(pkg);
        report.calculate_compliance();
        report.calculate_security_score();

        assert!(report.critical_vulnerabilities > 0);
        assert_eq!(report.compliance_status, ComplianceStatus::Remediation);
    }

    #[test]
    fn test_compliance_status_calculation() {
        let mut report = VulnerabilityScanReport::new();
        report.calculate_compliance();
        assert_eq!(report.compliance_status, ComplianceStatus::Pass);

        report.critical_vulnerabilities = 10;
        report.calculate_compliance();
        assert_eq!(report.compliance_status, ComplianceStatus::Fail);
    }

    #[test]
    fn test_security_score_calculation() {
        let mut report = VulnerabilityScanReport::new();
        let mut pkg = PackageScanResult::new("pkg", "1.0");

        let cve1 = Cve::new("CVE-2024-1", "pkg", 9.0, "Critical");
        pkg.add_vulnerability(cve1).unwrap();

        report.add_package_result(pkg);
        report.calculate_security_score();

        assert!(report.security_score < 100);
    }

    #[test]
    fn test_vulnerability_summary_generation() {
        let report = VulnerabilityScanReport::new();
        let summary = report.generate_summary();
        assert!(summary.contains("Vulnerability Scan Report"));
        assert!(summary.contains("Total Vulnerabilities: 0"));
    }

    // SIEM Logging Tests
    #[test]
    fn test_security_event_creation() {
        let event =
            SecurityEvent::new(SecurityEventType::AuthenticationAttempt, "web-app", "login");

        assert_eq!(event.event_type, SecurityEventType::AuthenticationAttempt);
        assert_eq!(event.source, "web-app");
        assert_eq!(event.action, "login");
    }

    #[test]
    fn test_event_type_severity_levels() {
        assert_eq!(
            SecurityEventType::AuthenticationAttempt.severity_level(),
            SeverityLevel::Informational
        );
        assert_eq!(
            SecurityEventType::ThreatDetected.severity_level(),
            SeverityLevel::Critical
        );
        assert_eq!(
            SecurityEventType::VulnerabilityDetected.severity_level(),
            SeverityLevel::Critical
        );
    }

    #[test]
    fn test_severity_level_syslog_numbers() {
        assert_eq!(SeverityLevel::Debug.syslog_number(), 7);
        assert_eq!(SeverityLevel::Informational.syslog_number(), 6);
        assert_eq!(SeverityLevel::Critical.syslog_number(), 3);
        assert_eq!(SeverityLevel::Emergency.syslog_number(), 1);
    }

    #[test]
    fn test_security_event_cef_format() {
        let event = SecurityEvent::new(SecurityEventType::DataAccess, "api-server", "read_config");

        let cef = event.to_cef();
        assert!(cef.starts_with("CEF:0"));
        assert!(cef.contains("api-server"));
    }

    #[test]
    fn test_security_event_leef_format() {
        let event = SecurityEvent::new(SecurityEventType::DataModification, "database", "update");

        let leef = event.to_leef();
        assert!(leef.starts_with("LEEF:2.0"));
        assert!(leef.contains("database"));
    }

    #[test]
    fn test_event_result_variants() {
        assert_eq!(EventResult::Success.as_str(), "SUCCESS");
        assert_eq!(EventResult::Failure.as_str(), "FAILURE");
        assert_eq!(EventResult::Partial.as_str(), "PARTIAL");
    }

    #[test]
    fn test_audit_trail_creation() {
        let mut trail = AuditTrail::new();

        let event = SecurityEvent::new(SecurityEventType::AuthenticationAttempt, "app", "login");

        trail.add_event(event).unwrap();
        assert_eq!(trail.event_count, 1);
    }

    #[test]
    fn test_audit_trail_event_filtering() {
        let mut trail = AuditTrail::new();

        let event1 = SecurityEvent::new(SecurityEventType::AuthenticationAttempt, "app", "login");
        let event2 = SecurityEvent::new(SecurityEventType::DataAccess, "db", "select");

        trail.add_event(event1).unwrap();
        trail.add_event(event2).unwrap();

        let auth_events = trail.get_events_by_type(SecurityEventType::AuthenticationAttempt);
        assert_eq!(auth_events.len(), 1);
    }

    #[test]
    fn test_siem_logger_event_logging() {
        let mut logger = SiemLogger::new(LogFormat::JSON);

        let event = SecurityEvent::new(SecurityEventType::PolicyViolation, "app", "policy_check");

        assert!(logger.log_event(event).is_ok());
        assert!(logger.get_trail().event_count > 0);
    }

    #[test]
    fn test_siem_logger_formatting() {
        let logger = SiemLogger::new(LogFormat::CEF);

        let event = SecurityEvent::new(SecurityEventType::ThreatDetected, "ids", "alert");

        let formatted = logger.format_event(&event);
        assert!(!formatted.is_empty());
    }

    #[test]
    fn test_critical_events_retrieval() {
        let mut logger = SiemLogger::new(LogFormat::JSON);

        let event = SecurityEvent::new(SecurityEventType::ThreatDetected, "ids", "alert");

        logger.log_event(event).unwrap();

        let critical = logger.get_critical_events();
        assert!(!critical.is_empty());
    }

    // Security Policy Tests
    #[test]
    fn test_security_standards() {
        assert_eq!(SecurityStandard::PciDss.as_str(), "PCI-DSS");
        assert_eq!(SecurityStandard::Hipaa.as_str(), "HIPAA");
        assert_eq!(SecurityStandard::Soc2.as_str(), "SOC2");
        assert_eq!(SecurityStandard::Gdpr.as_str(), "GDPR");
    }

    #[test]
    fn test_requirement_priority_ordering() {
        assert!(RequirementPriority::Critical > RequirementPriority::High);
        assert!(RequirementPriority::High > RequirementPriority::Medium);
        assert!(RequirementPriority::Medium > RequirementPriority::Low);
    }

    #[test]
    fn test_security_policy_creation() {
        let mut policy = SecurityPolicy::new("pol-auth", "Authentication Policy");
        policy.add_standard(SecurityStandard::Hipaa);

        assert_eq!(policy.id, "pol-auth");
        assert!(policy.standards.contains(&SecurityStandard::Hipaa));
    }

    #[test]
    fn test_policy_requirement_addition() {
        let mut policy = SecurityPolicy::new("pol-1", "Policy");

        let req = PolicyRequirement {
            id: "req-1".to_string(),
            title: "MFA Required".to_string(),
            description: "All users must use MFA".to_string(),
            standard: SecurityStandard::PciDss,
            priority: RequirementPriority::Critical,
            mandatory: true,
            check_id: "check-mfa".to_string(),
        };

        assert!(policy.add_requirement(req).is_ok());
        assert_eq!(policy.requirements.len(), 1);
    }

    #[test]
    fn test_policy_mandatory_requirements_filtering() {
        let mut policy = SecurityPolicy::new("pol-1", "Policy");

        let req1 = PolicyRequirement {
            id: "req-1".to_string(),
            title: "Mandatory".to_string(),
            description: "Mandatory req".to_string(),
            standard: SecurityStandard::PciDss,
            priority: RequirementPriority::High,
            mandatory: true,
            check_id: "check-1".to_string(),
        };

        let req2 = PolicyRequirement {
            id: "req-2".to_string(),
            title: "Optional".to_string(),
            description: "Optional req".to_string(),
            standard: SecurityStandard::PciDss,
            priority: RequirementPriority::Low,
            mandatory: false,
            check_id: "check-2".to_string(),
        };

        policy.add_requirement(req1).unwrap();
        policy.add_requirement(req2).unwrap();

        let mandatory = policy.get_mandatory_requirements();
        assert_eq!(mandatory.len(), 1);
    }

    #[test]
    fn test_compliance_check_result_creation() {
        let passed = ComplianceCheckResult::passed("check-1", "req-1");
        assert!(passed.passed);

        let failed = ComplianceCheckResult::failed("check-2", "req-2", "Failed condition");
        assert!(!failed.passed);
        assert!(failed.failure_reason.is_some());
    }

    #[test]
    fn test_compliance_report_creation() {
        let mut report = ComplianceReport::new("pol-1");

        let result1 = ComplianceCheckResult::passed("check-1", "req-1");
        let result2 = ComplianceCheckResult::failed("check-2", "req-2", "Not compliant");

        report.add_result(result1);
        report.add_result(result2);

        report.calculate_compliance();

        assert_eq!(report.passed_checks, 1);
        assert_eq!(report.failed_checks, 1);
        assert!(!report.compliant);
        assert_eq!(report.compliance_percentage, 50.0);
    }

    #[test]
    fn test_compliance_report_summary() {
        let mut report = ComplianceReport::new("pol-1");
        let result = ComplianceCheckResult::passed("check-1", "req-1");
        report.add_result(result);
        report.calculate_compliance();

        let summary = report.generate_summary();
        assert!(summary.contains("Compliance Report Summary"));
        assert!(summary.contains("Passed Checks: 1"));
    }

    #[test]
    fn test_policy_compliance_manager() {
        let mut manager = PolicyComplianceManager::new();
        let policy = SecurityPolicy::new("pol-1", "Main Policy");

        assert!(manager.register_policy(policy).is_ok());
        assert!(manager.get_policy("pol-1").is_ok());
    }

    #[test]
    fn test_policy_list_and_count() {
        let mut manager = PolicyComplianceManager::new();

        let policy1 = SecurityPolicy::new("pol-1", "Policy 1");
        let policy2 = SecurityPolicy::new("pol-2", "Policy 2");

        manager.register_policy(policy1).unwrap();
        manager.register_policy(policy2).unwrap();

        let policies = manager.list_policies();
        assert_eq!(policies.len(), 2);
    }

    #[test]
    fn test_vulnerability_severity_distribution() {
        let mut result = PackageScanResult::new("pkg", "1.0");

        let cve_high = Cve::new("CVE-1", "pkg", 7.5, "High");
        let cve_medium = Cve::new("CVE-2", "pkg", 5.0, "Medium");

        result.add_vulnerability(cve_high).unwrap();
        result.add_vulnerability(cve_medium).unwrap();

        let dist = result.severity_distribution();
        assert!(dist.contains_key("HIGH"));
        assert!(dist.contains_key("MEDIUM"));
    }

    #[test]
    fn test_cve_with_advisory_urls() {
        let mut cve = Cve::new("CVE-2024-2222", "pkg", 8.0, "Critical");
        cve.advisory_urls
            .push("https://nvd.nist.gov/vuln/detail/CVE-2024-2222".to_string());

        assert!(!cve.advisory_urls.is_empty());
    }

    #[test]
    fn test_siem_event_extensions() {
        let mut event = SecurityEvent::new(SecurityEventType::DataAccess, "api", "read");

        event.set_extension("request_id", "req-12345");
        event.set_extension("ip_address", "192.168.1.100");

        assert_eq!(event.extensions.len(), 2);
    }

    #[test]
    fn test_event_compliance_marking() {
        let mut event = SecurityEvent::new(SecurityEventType::DataModification, "db", "update");

        event.mark_compliance_relevant();
        assert!(event.compliance_relevant);
    }

    #[test]
    fn test_multi_standard_policy() {
        let mut policy = SecurityPolicy::new("multi", "Multi-Standard");
        policy.add_standard(SecurityStandard::PciDss);
        policy.add_standard(SecurityStandard::Hipaa);
        policy.add_standard(SecurityStandard::Soc2);

        assert_eq!(policy.standards.len(), 3);
    }
}
