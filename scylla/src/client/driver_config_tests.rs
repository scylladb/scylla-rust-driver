use super::{DriverConfigReporter, MAX_REPORT_SIZE};

#[test]
fn default_report_is_minimal() {
    let report = DriverConfigReporter::new().report(true).unwrap();
    assert_eq!(report, r#"{"version":1}"#);
    assert!(report.len() < MAX_REPORT_SIZE / 100);
}
