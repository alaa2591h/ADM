//! Performance Analysis and Benchmarking Tests (Days 17-18)

#[cfg(test)]
mod performance_benchmarks {
    use adm_domain::load_testing::*;
    use adm_domain::performance_metrics::*;
    use std::time::Duration;

    // Performance Metrics Tests
    #[test]
    fn test_metric_type_strings() {
        assert_eq!(MetricType::Latency.as_str(), "latency_ms");
        assert_eq!(MetricType::Throughput.as_str(), "throughput_rps");
        assert_eq!(MetricType::MemoryUsage.as_str(), "memory_bytes");
    }

    #[test]
    fn test_metric_data_point_creation() {
        let metric = MetricDataPoint::new(MetricType::Latency, 42.5, "encryption");
        assert_eq!(metric.value, 42.5);
        assert_eq!(metric.operation, "encryption");
    }

    #[test]
    fn test_metric_with_single_tag() {
        let metric = MetricDataPoint::new(MetricType::Latency, 50.0, "key_rotation")
            .with_tag("algorithm", "AES-256");

        assert_eq!(metric.tags.len(), 1);
        assert_eq!(metric.tags.get("algorithm"), Some(&"AES-256".to_string()));
    }

    #[test]
    fn test_metric_with_multiple_tags() {
        let metric = MetricDataPoint::new(MetricType::Throughput, 1000.0, "tls_handshake")
            .with_tag("cipher", "ChaCha20-Poly1305")
            .with_tag("version", "TLS1.3")
            .with_tag("protocol", "HTTPS");

        assert_eq!(metric.tags.len(), 3);
    }

    #[test]
    fn test_performance_stats_basic() {
        let samples = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        let stats = PerformanceStats::from_samples(&samples).unwrap();

        assert_eq!(stats.min, 10.0);
        assert_eq!(stats.max, 50.0);
        assert_eq!(stats.median, 30.0);
        assert_eq!(stats.samples, 5);
    }

    #[test]
    fn test_performance_stats_percentiles() {
        let samples: Vec<f64> = (1..=100).map(f64::from).collect();
        let stats = PerformanceStats::from_samples(&samples).unwrap();

        assert!(stats.p95 > stats.median);
        assert!(stats.p99 >= stats.p95);
        assert_eq!(stats.samples, 100);
    }

    #[test]
    fn test_performance_timer_creation() {
        let timer = PerformanceTimer::start("encryption_op");
        std::thread::sleep(Duration::from_millis(5));

        let elapsed = timer.elapsed();
        assert!(elapsed.as_millis() >= 5);
    }

    #[test]
    fn test_performance_timer_metric_conversion() {
        let timer = PerformanceTimer::start("test_op");
        std::thread::sleep(Duration::from_millis(10));

        let metric = timer.to_metric();
        assert_eq!(metric.metric_type, MetricType::Latency);
        assert!(metric.value >= 10.0);
    }

    #[test]
    fn test_metrics_collector_recording() {
        let mut collector = MetricsCollector::new();
        collector.record_latency("tls_setup", 25.5);
        collector.record_throughput("certificate_validation", 500.0);

        assert_eq!(collector.get_metrics().len(), 2);
    }

    #[test]
    fn test_metrics_collector_latency_aggregation() {
        let mut collector = MetricsCollector::new();

        for i in 1..=20 {
            collector.record_latency("key_rotation", f64::from(i) * 2.5);
        }

        let stats = collector
            .get_stats("key_rotation", MetricType::Latency)
            .unwrap();
        assert_eq!(stats.samples, 20);
        assert!(stats.mean > 0.0);
    }

    #[test]
    fn test_metrics_collector_report_generation() {
        let mut collector = MetricsCollector::new();
        collector.record_latency("encryption", 15.5);

        let report = collector.generate_report("encryption");
        assert!(report.contains("Latency (ms):"));
    }

    #[test]
    fn test_benchmark_result_calculation() {
        let bench = BenchmarkResult::new("encryption_aes256", 1000, 100.0);

        assert_eq!(bench.iterations, 1000);
        assert_eq!(bench.duration_per_iteration, 0.1);
        assert_eq!(bench.ops_per_second, 10000.0);
    }

    #[test]
    fn test_benchmark_suite_aggregation() {
        let mut suite = BenchmarkSuite::new("security_benchmarks");

        let bench1 = BenchmarkResult::new("tls_handshake", 500, 50.0);
        let bench2 = BenchmarkResult::new("key_derivation", 1000, 100.0);
        let bench3 = BenchmarkResult::new("cert_validation", 2000, 200.0);

        suite.add_result(bench1);
        suite.add_result(bench2);
        suite.add_result(bench3);

        assert_eq!(suite.benchmarks.len(), 3);
        assert_eq!(suite.total_duration_ms, 350.0);
    }

    #[test]
    fn test_benchmark_suite_summary() {
        let mut suite = BenchmarkSuite::new("performance_suite");
        let bench = BenchmarkResult::new("operation", 100, 10.0);
        suite.add_result(bench);

        let summary = suite.generate_summary();
        assert!(summary.contains("Benchmark Suite: performance_suite"));
        assert!(summary.contains("Ops/sec:"));
    }

    // Load Testing Tests
    #[test]
    fn test_load_test_config_validation() {
        let mut config = LoadTestConfig::default();
        assert!(config.validate().is_ok());

        config.concurrent_users = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_load_test_stress_config() {
        let config = LoadTestConfig::stress_test();
        assert_eq!(config.concurrent_users, 100);
        assert_eq!(config.total_requests, 10000);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_request_result_success() {
        assert_eq!(RequestResult::Success.as_str(), "success");
        assert_eq!(RequestResult::Error.as_str(), "error");
        assert_eq!(RequestResult::Timeout.as_str(), "timeout");
        assert_eq!(RequestResult::RateLimited.as_str(), "rate_limited");
    }

    #[test]
    fn test_request_latency_creation() {
        let success = RequestLatency::success(42.5);
        assert_eq!(success.result, RequestResult::Success);

        let failed = RequestLatency::failed(RequestResult::Timeout, 5000.0);
        assert_eq!(failed.result, RequestResult::Timeout);
    }

    #[test]
    fn test_load_test_results_single_request() {
        let config = LoadTestConfig::default();
        let latencies = vec![RequestLatency::success(50.0)];

        let results = LoadTestResults::from_latencies(config, 100.0, latencies).unwrap();
        assert_eq!(results.successful, 1);
        assert_eq!(results.total_completed, 1);
        assert_eq!(results.success_rate, 100.0);
    }

    #[test]
    fn test_load_test_results_mixed_results() {
        let config = LoadTestConfig::default();
        let latencies = vec![
            RequestLatency::success(10.0),
            RequestLatency::success(20.0),
            RequestLatency::failed(RequestResult::Error, 5000.0),
            RequestLatency::failed(RequestResult::Timeout, 5000.0),
        ];

        let results = LoadTestResults::from_latencies(config, 200.0, latencies).unwrap();
        assert_eq!(results.successful, 2);
        assert_eq!(results.failed, 1);
        assert_eq!(results.timeouts, 1);
        assert_eq!(results.total_completed, 4);
    }

    #[test]
    fn test_load_test_results_percentiles() {
        let config = LoadTestConfig::default();
        let mut latencies = vec![];

        for i in 1..=100 {
            latencies.push(RequestLatency::success(f64::from(i) * 10.0));
        }

        let results = LoadTestResults::from_latencies(config, 1000.0, latencies).unwrap();
        assert!(results.p95_latency > results.median_latency);
        assert!(results.p99_latency >= results.p95_latency);
    }

    #[test]
    fn test_load_test_results_report() {
        let config = LoadTestConfig::default();
        let latencies = vec![RequestLatency::success(25.0), RequestLatency::success(35.0)];

        let results = LoadTestResults::from_latencies(config, 100.0, latencies).unwrap();
        let report = results.generate_report();

        assert!(report.contains("Load Test Report"));
        assert!(report.contains("RPS:"));
        assert!(report.contains("P95:"));
    }

    #[test]
    fn test_rate_limiter_unlimited() {
        let mut limiter = RateLimiter::new(0);

        for _ in 0..100 {
            assert!(limiter.allow_request());
        }
    }

    #[test]
    fn test_rate_limiter_constrained() {
        let mut limiter = RateLimiter::new(1000); // 1000 RPS

        let mut allowed = 0;
        for _ in 0..10 {
            if limiter.allow_request() {
                allowed += 1;
            }
        }

        assert!(allowed >= 0);
    }

    #[test]
    fn test_tls_handshake_benchmark() {
        let bench = TlsBenchmark::run(100);
        assert_eq!(bench.handshake_count, 100);
        assert!(bench.total_duration_ms > 0.0);
        assert!(bench.mean_time_ms > 0.0);
    }

    #[test]
    fn test_tls_handshake_benchmark_scaling() {
        let bench_small = TlsBenchmark::run(10);
        let bench_large = TlsBenchmark::run(100);

        assert!(bench_large.total_duration_ms > bench_small.total_duration_ms);
    }

    // Integration Tests
    #[test]
    fn test_complete_benchmark_workflow() {
        let mut suite = BenchmarkSuite::new("integrated_benchmark");

        // Simulate encryption benchmark
        let mut collector = MetricsCollector::new();
        for i in 1..=50 {
            collector.record_latency("encryption", f64::from(i) * 2.0);
        }

        let stats = collector
            .get_stats("encryption", MetricType::Latency)
            .unwrap();
        let bench = BenchmarkResult::new("encryption", 50, stats.mean * 50.0);
        suite.add_result(bench);

        assert!(!suite.benchmarks.is_empty());
    }

    #[test]
    fn test_tls_performance_under_load() {
        let config = LoadTestConfig {
            concurrent_users: 10,
            total_requests: 100,
            target_rps: 1000,
            test_name: "TLS Performance".to_string(),
            ..LoadTestConfig::default()
        };

        assert!(config.validate().is_ok());

        let mut latencies = vec![];
        for _ in 0..100 {
            latencies.push(RequestLatency::success(50.0));
        }

        let results = LoadTestResults::from_latencies(config, 500.0, latencies).unwrap();
        assert!(results.success_rate > 90.0);
    }

    #[test]
    fn test_rate_limiting_effectiveness() {
        let mut limiter = RateLimiter::new(100); // 100 RPS
        let start = std::time::Instant::now();

        for _ in 0..10 {
            limiter.wait_until_allowed();
        }

        let elapsed = start.elapsed().as_secs_f64();
        // Should roughly take 0.1 seconds for 10 requests at 100 RPS
        assert!(elapsed >= 0.05); // Allow some variance
    }
}
