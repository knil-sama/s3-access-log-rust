use csv::ReaderBuilder;

use http::StatusCode;
use std::net::IpAddr;
use std::net::Ipv4Addr;

use std::fmt;
use serde::{Deserialize, Serialize};
use serde::de::{self, Visitor, Deserializer};
use serde::ser::{SerializeStruct, Serializer};
use std::fmt::Display;
use std::marker::PhantomData;
use std::str::FromStr;
use serde_with::{DeserializeAs, SerializeAs};
use serde_with::serde_as;
use std::num::NonZeroU32;
// copy paste from https://docs.rs/serde_with/latest/src/serde_with/de/impls.rs.html#939-981
pub struct DefaultStringToNone;

impl<'de, Str> DeserializeAs<'de, Option<Str>> for DefaultStringToNone
where
    Str: FromStr,
    Str::Err: Display,
{
    fn deserialize_as<D>(deserializer: D) -> Result<Option<Str>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OptionStringEmptyNone<S>(PhantomData<S>);
        impl<'de, S> Visitor<'de> for OptionStringEmptyNone<S>
        where
            S: FromStr,
            S::Err: Display,
        {
            type Value = Option<S>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "-" => Ok(None),
                    v => S::from_str(v).map(Some).map_err(de::Error::custom),
                }
            }

            // handles the `null` case
            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(None)
            }
        }

        deserializer.deserialize_any(OptionStringEmptyNone(PhantomData))
    }
}

impl<T> SerializeAs<Option<T>> for DefaultStringToNone
where
    T: Display,
{
    fn serialize_as<S>(source: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(value) = source {
            serializer.collect_str(value)
        } else {
            serializer.serialize_str("-")
        }
    }
}

pub struct DefaultIntToNone;

impl<'de, U32> DeserializeAs<'de, Option<U32>> for DefaultIntToNone
where
    U32: FromStr,
    U32::Err: Display,
{
    fn deserialize_as<D>(deserializer: D) -> Result<Option<U32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OptionIntEmptyNone<S>(PhantomData<S>);
        impl<'de, S> Visitor<'de> for OptionIntEmptyNone<S>
        where
            S: FromStr,
            S::Err: Display,        
        {
            type Value = Option<S>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string truc")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "-" => Ok(None),
                    v => return S::from_str(v).map(Some).map_err(de::Error::custom),
                }
            }
            //S::from_str("1").map(Some).map_err(de::Error::custom)
            // handles the `null` case
            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(None)
            }
        }

        deserializer.deserialize_any(OptionIntEmptyNone(PhantomData))
    }
}

impl<T> SerializeAs<Option<T>> for DefaultIntToNone
where
    T: Display,
{
    fn serialize_as<S>(source: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(value) = source {
            serializer.collect_str(value)
        } else {
            serializer.serialize_str("-")
        }
    }
}


pub fn deserialize_number_from_string<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr + serde::Deserialize<'de>,
    <T as FromStr>::Err: Display,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrInt<T> {
        String(String),
        Number(T),
    }

    match StringOrInt::<T>::deserialize(deserializer)? {
        StringOrInt::String(s) => {
            if s == "-" {
                Ok(None)
            } else {
                s.parse::<T>().map(Some).map_err(serde::de::Error::custom)
            }
        },
        StringOrInt::Number(i) => Ok(Some(i)),
    }
}

#[serde_as]
#[derive(Debug, Deserialize,PartialEq)]
struct S3AccessLogRecord {
    bucket_owner: String,
    bucket_name: String,
    time: String,
    remote_ip: IpAddr,
    #[serde_as(as = "DefaultStringToNone")]
    requester: Option<String>, //The canonical user ID of the requester, or a - for unauthenticated requests. If the requester was an IAM user, this field returns the requester's IAM user name along with the AWS account root user that the IAM user belongs to. This identifier is the same one used for access control purposes.
    request_id: String, //A string generated by Amazon S3 to uniquely identify each request.
    operation: String,
    key: String,
    request_uri: String,
    #[serde(with = "http_serde::status_code")]
    http_status: StatusCode,
    error_code: String,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    bytes_sent: Option<u64>, // The number of response bytes sent, excluding HTTP protocol overhead, or - if zero. WTF !!!
    #[serde(deserialize_with = "deserialize_number_from_string")]
    object_size: Option<u64>, // can also be - but the doc don't mention it !!!
    total_time: u64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    turn_around_time: Option<u64>,
    referer: String,
    user_agent: String,
    version_id: String,
    host_id: String,
    signature_version: String,
    cipher_suite: String,
    authentication_type: String,
    host_header: String,
    #[serde_as(as = "DefaultStringToNone")]
    tls_version: Option<String>,
    #[serde_as(as = "DefaultStringToNone")]
    access_point_arn: Option<String>,
    #[serde_as(as = "DefaultStringToNone")]
    acl_required: Option<String>
}

fn convert_wsc_str_to_s3_access_log_record(wsv: &str) -> Vec<S3AccessLogRecord> {
    let valid_wsv = wsv.replace("[", "\"").replace("]", "\"");
    let mut reader = ReaderBuilder::new().has_headers(false).delimiter(b' ').from_reader(valid_wsv.as_bytes());
     reader.deserialize::<S3AccessLogRecord>().map(|res| res.expect("error will parsing csv content")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        let expected_result = S3AccessLogRecord {bucket_owner : "7e1c2dcc1527ebbd9a81efbefb6a7d5945b7c6fe00160f682c2b7c056d301e83".to_string(),
        bucket_name: "aws-website-demonchy-5v3aj".to_string(),
        time: "11/Nov/2023:03:37:50 +0000".to_string(),
        remote_ip: std::net::IpAddr::V4(Ipv4Addr::new(130,176,48,151)),
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
        host_id: "m3PGwDN1s8smqpOSEELewHILMcdm7xri7/UsWHBhRrT0w23Pp0YcEmgboXyHFTv7qR7RvFMrUgo=".to_string(),
        signature_version: "-".to_string(),
        cipher_suite: "-".to_string(),
        authentication_type: "-".to_string(),
        host_header: "aws-website-demonchy-5v3aj.s3-website-us-east-1.amazonaws.com".to_string(),
        tls_version: None,
        access_point_arn: None,
        acl_required: None,
    };
        let wsv = "7e1c2dcc1527ebbd9a81efbefb6a7d5945b7c6fe00160f682c2b7c056d301e83 aws-website-demonchy-5v3aj [11/Nov/2023:03:37:50 +0000] 130.176.48.151 - YDYP07R0QHFNH76W WEBSITE.GET.OBJECT favicon.ico \"GET /favicon.ico HTTP/1.1\" 404 NoSuchKey 346 - 39 - \"-\" \"Amazon CloudFront\" - m3PGwDN1s8smqpOSEELewHILMcdm7xri7/UsWHBhRrT0w23Pp0YcEmgboXyHFTv7qR7RvFMrUgo= - - - aws-website-demonchy-5v3aj.s3-website-us-east-1.amazonaws.com - - -";
        assert_eq!(convert_wsc_str_to_s3_access_log_record(&wsv).into_iter().next().unwrap(), expected_result);

    }
}
