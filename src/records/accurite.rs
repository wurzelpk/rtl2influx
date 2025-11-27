use serde::Deserialize;
use serde::de::{self, Deserializer, Visitor};
use std::fmt;

/// Represents an Acurite "tower" record as JSON-deserialized with serde_json.
#[derive(Debug, Deserialize)]
pub struct AccuriteRecord {
    // JSON: "time": "2025-11-21 01:26:21"
    pub time: String,

    // JSON: "model": "Acurite-Tower"
    pub model: String,

    // JSON: "id": 10956
    pub id: u64,

    // JSON: "channel": "A"
    pub channel: String,

    // JSON: "battery_ok": 1  (deserialize 0/1 or bool -> bool)
    #[serde(deserialize_with = "de_bool_from_int")]
    pub battery_ok: bool,

    // JSON: "temperature_C": 22.700
    #[serde(rename = "temperature_C")]
    pub temperature_c: f64,

    // JSON: "humidity": 55
    pub humidity: u8,

    // JSON: "mic": "CHECKSUM"
    pub mic: String,
}

fn de_bool_from_int<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    struct BoolVisitor;

    impl<'de> Visitor<'de> for BoolVisitor {
        type Value = bool;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "a boolean or an integer 0/1 (or their string forms)")
        }

        fn visit_bool<E>(self, v: bool) -> Result<bool, E> {
            Ok(v)
        }

        fn visit_u64<E>(self, v: u64) -> Result<bool, E>
        where
            E: de::Error,
        {
            Ok(v != 0)
        }

        fn visit_i64<E>(self, v: i64) -> Result<bool, E>
        where
            E: de::Error,
        {
            Ok(v != 0)
        }

        fn visit_str<E>(self, v: &str) -> Result<bool, E>
        where
            E: de::Error,
        {
            match v {
                "1" | "true" | "True" => Ok(true),
                "0" | "false" | "False" => Ok(false),
                other => Err(E::custom(format!("invalid boolean string: {}", other))),
            }
        }
    }

    deserializer.deserialize_any(BoolVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deserialize_sample() {
        let j = json!({
            "time": "2025-11-21 01:26:21",
            "model": "Acurite-Tower",
            "id": 10956,
            "channel": "A",
            "battery_ok": 1,
            "temperature_C": 22.700,
            "humidity": 55,
            "mic": "CHECKSUM"
        });

        let s = j.to_string();
        let rec: AccuriteRecord = serde_json::from_str(&s).expect("deserialize");
        assert_eq!(rec.model, "Acurite-Tower");
        assert!(rec.battery_ok);
        assert_eq!(rec.temperature_c, 22.7);
    }

    #[test]
    fn deserialize_sample_string() {
        let j = r#"{"time" : "2025-11-21 01:26:21", "model" : "Acurite-Tower", "id" : 10956, "A": "b",
                    "channel" : "A", "battery_ok" : 1, "temperature_C" : 22.700, "humidity" : 55, "mic" : "CHECKSUM"}"#;
        let rec: AccuriteRecord = serde_json::from_str(j).expect("deserialize");
        assert_eq!(rec.model, "Acurite-Tower");
        assert!(rec.battery_ok);
        assert_eq!(rec.temperature_c, 22.7);
        assert_eq!(rec.humidity, 55);
    }
}
