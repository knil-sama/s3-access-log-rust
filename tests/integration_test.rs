use chrono::{DateTime, NaiveDateTime, Utc};
use http::StatusCode;
use s3_access_log_rust;
use std::net::Ipv4Addr;

#[test]
fn it_convert_s3_access_log_file() {
    let dt = NaiveDateTime::parse_from_str("11/Nov/2023:03:37:50 +0000", "%d/%b/%Y:%H:%M:%S %z")
        .unwrap();
    let expected_result = s3_access_log_rust::S3AccessLogRecord {
        bucket_owner: "7e1c2dcc1527ebbd9a81efbefb6a7d5945b7c6fe00160f682c2b7c056d301e83"
            .to_string(),
        bucket_name: "aws-website-demonchy-5v3aj".to_string(),
        time: DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc),
        remote_ip: Some(std::net::IpAddr::V4(Ipv4Addr::new(130, 176, 48, 151))),
        requester: None,
        request_id: "YDYP07R0QHFNH76W".to_string(),
        operation: "WEBSITE.GET.OBJECT".to_string(),
        key: "favicon.ico".to_string(),
        request_uri: "GET /favicon.ico HTTP/1.1".to_string(),
        http_status: StatusCode::NOT_FOUND,
        error_code: "NoSuchKey".to_string(),
        bytes_sent: Some(346),
        object_size: None,
        total_time: 39,
        turn_around_time: None,
        referer: "-".to_string(),
        user_agent: "Amazon CloudFront".to_string(),
        version_id: "-".to_string(),
        host_id: "m3PGwDN1s8smqpOSEELewHILMcdm7xri7/UsWHBhRrT0w23Pp0YcEmgboXyHFTv7qR7RvFMrUgo="
            .to_string(),
        signature_version: "-".to_string(),
        cipher_suite: "-".to_string(),
        authentication_type: "-".to_string(),
        host_header: "aws-website-demonchy-5v3aj.s3-website-us-east-1.amazonaws.com".to_string(),
        tls_version: None,
        access_point_arn: None,
        acl_required: None,
    };
    let wsv = "7e1c2dcc1527ebbd9a81efbefb6a7d5945b7c6fe00160f682c2b7c056d301e83 aws-website-demonchy-5v3aj [11/Nov/2023:03:37:50 +0000] 130.176.48.151 - YDYP07R0QHFNH76W WEBSITE.GET.OBJECT favicon.ico \"GET /favicon.ico HTTP/1.1\" 404 NoSuchKey 346 - 39 - \"-\" \"Amazon CloudFront\" - m3PGwDN1s8smqpOSEELewHILMcdm7xri7/UsWHBhRrT0w23Pp0YcEmgboXyHFTv7qR7RvFMrUgo= - - - aws-website-demonchy-5v3aj.s3-website-us-east-1.amazonaws.com - - -";
    assert_eq!(
        s3_access_log_rust::convert_wsc_str_to_s3_access_log_record(&wsv)
            .into_iter()
            .next()
            .unwrap(),
        expected_result
    );
}
