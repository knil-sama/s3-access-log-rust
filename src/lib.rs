use csv::ReaderBuilder;

use http::StatusCode;
use std::net::IpAddr;

use chrono::{DateTime, Utc};
use serde::de::{self, Deserializer, Visitor};
use serde::ser::Serializer;
use serde::Deserialize;
use serde_with::serde_as;
use serde_with::{DeserializeAs, SerializeAs};
use std::fmt;
use std::fmt::Display;
use std::marker::PhantomData;
use std::option::Option;
use std::str::FromStr;

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
        }
        StringOrInt::Number(i) => Ok(Some(i)),
    }
}

//https://users.rust-lang.org/t/serde-serialization-with-option-of-external-struct/67746/2
//https://play.rust-lang.org/?version=stable&mode=debug&edition=2021&gist=f42e6ff34e6171af99c432dbbd3b8df1

// copy paste from https://docs.rs/serde_with/latest/src/serde_with/de/impls.rs.html#939-981
pub struct DefaultIpAddrToNone;

impl<'de, T> DeserializeAs<'de, Option<T>> for DefaultIpAddrToNone
where
    T: FromStr,
    T::Err: Display,
{
    fn deserialize_as<D>(deserializer: D) -> Result<Option<T>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OptionIpAddrToNone<S>(PhantomData<S>);
        impl<'de, S> Visitor<'de> for OptionIpAddrToNone<S>
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
        }

        deserializer.deserialize_any(OptionIpAddrToNone(PhantomData))
    }
}

impl<T> SerializeAs<Option<T>> for DefaultIpAddrToNone
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

// from https://serde.rs/custom-date-format.html#date-in-a-custom-format
mod my_date_format {
    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    const FORMAT: &str = "%d/%b/%Y:%H:%M:%S %z";

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let dt = NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)?;
        Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
    }
}

#[serde_as]
#[derive(Debug, Deserialize, PartialEq)]
pub struct S3AccessLogRecord {
    pub bucket_owner: String,
    pub bucket_name: String,
    #[serde(with = "my_date_format")]
    pub time: DateTime<Utc>,
    #[serde_as(as = "DefaultIpAddrToNone")]
    pub remote_ip: Option<IpAddr>,
    #[serde_as(as = "DefaultStringToNone")]
    pub requester: Option<String>, //The canonical user ID of the requester, or a - for unauthenticated requests. If the requester was an IAM user, this field returns the requester's IAM user name along with the AWS account root user that the IAM user belongs to. This identifier is the same one used for access control purposes.
    pub request_id: String, //A string generated by Amazon S3 to uniquely identify each request.
    pub operation: String,
    pub key: String,
    pub request_uri: String,
    #[serde(with = "http_serde::status_code")]
    pub http_status: StatusCode,
    pub error_code: String,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub bytes_sent: Option<u64>, // The number of response bytes sent, excluding HTTP protocol overhead, or - if zero. WTF !!!
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub object_size: Option<u64>, // can also be - but the doc don't mention it !!!
    pub total_time: u64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub turn_around_time: Option<u64>,
    pub referer: String,
    pub user_agent: String,
    pub version_id: String,
    pub host_id: String,
    pub signature_version: String,
    pub cipher_suite: String,
    pub authentication_type: String,
    pub host_header: String,
    #[serde_as(as = "DefaultStringToNone")]
    pub tls_version: Option<String>,
    #[serde_as(as = "DefaultStringToNone")]
    pub access_point_arn: Option<String>,
    #[serde_as(as = "DefaultStringToNone")]
    pub acl_required: Option<String>,
}

pub fn convert_wsc_str_to_s3_access_log_record(wsv: &str) -> Vec<S3AccessLogRecord> {
    let valid_wsv = wsv.replace(['[', ']'], "\"");
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b' ')
        .from_reader(valid_wsv.as_bytes());
    reader
        .deserialize::<S3AccessLogRecord>()
        .map(|res| res.expect("error will parsing csv content"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::Serialize;
    use serde_test::assert_de_tokens_error;
    use std::net::Ipv4Addr;
    //use claims::assert_ok_eq;
    use serde::Deserialize;
    //use serde_assert::{Deserializer, Token};

    #[derive(Debug, PartialEq, Deserialize, Serialize)]
    struct DeserializeNumberFromStringTest {
        #[serde(deserialize_with = "deserialize_number_from_string")]
        string_as_number: Option<u64>,
    }

    #[test]
    fn it_instanciate_s3_access_log_record_struct() {
        let dt =
            NaiveDateTime::parse_from_str("11/Nov/2023:03:37:50 +0000", "%d/%b/%Y:%H:%M:%S %z")
                .unwrap();
        S3AccessLogRecord {
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
            host_header: "aws-website-demonchy-5v3aj.s3-website-us-east-1.amazonaws.com"
                .to_string(),
            tls_version: None,
            access_point_arn: None,
            acl_required: None,
        };
    }

    #[test]
    fn it_deserialize_number_from_string_convert_negative_number_to_u64_error() {
        assert_de_tokens_error::<DeserializeNumberFromStringTest>(
            &[serde_test::Token::I8(-14)],
            "invalid type: integer `-14`, expected struct DeserializeNumberFromStringTest",
        )
    }
}
