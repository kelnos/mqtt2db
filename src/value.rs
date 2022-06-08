use influxdb::Type;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::fmt;

#[derive(Copy, Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ValueType {
    Boolean,
    Float,
    SignedInteger,
    UnsignedInteger,
    Text,
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ValueType::*;
        let s = match self {
            Boolean => "boolean",
            Float => "float",
            SignedInteger => "signed integer",
            UnsignedInteger => "unsigned integer",
            Text => "text",
        };
        write!(f, "{}", s)
    }
}

pub trait ToInfluxType {
    fn to_influx_type(&self, value_type: ValueType) -> anyhow::Result<Type>;
}

impl ToInfluxType for String {
    fn to_influx_type(&self, value_type: ValueType) -> anyhow::Result<Type> {
        match value_type {
            ValueType::Boolean if self == "true" => Ok(Type::Boolean(true)),
            ValueType::Boolean if self == "false" => Ok(Type::Boolean(false)),
            ValueType::Boolean => Err(anyhow!("Value '{}' is not a valid boolean", self)),
            ValueType::Float => self
                .parse::<f64>()
                .map(|v| Type::Float(v))
                .map_err(|err| err.into()),
            ValueType::SignedInteger => self
                .parse::<i64>()
                .map(|v| Type::SignedInteger(v))
                .map_err(|err| err.into()),
            ValueType::UnsignedInteger => self
                .parse::<u64>()
                .map(|v| Type::UnsignedInteger(v))
                .map_err(|err| err.into()),
            ValueType::Text => Ok(Type::Text(self.clone())),
        }
    }
}

impl ToInfluxType for JsonValue {
    fn to_influx_type(&self, value_type: ValueType) -> anyhow::Result<Type> {
        match (value_type, self) {
            (ValueType::Boolean, JsonValue::Bool(true)) => Ok(Type::Boolean(true)),
            (ValueType::Boolean, JsonValue::Bool(false)) => Ok(Type::Boolean(false)),
            (ValueType::Float, JsonValue::Number(num)) => num
                .as_f64()
                .ok_or_else(|| anyhow!("Cannot be expressed as f64: {}", num))
                .map(|v| Type::Float(v)),
            (ValueType::SignedInteger, JsonValue::Number(num)) => num
                .as_i64()
                .ok_or_else(|| anyhow!("Cannot be expressed as i64: {}", num))
                .map(|v| Type::SignedInteger(v)),
            (ValueType::UnsignedInteger, JsonValue::Number(num)) => num
                .as_u64()
                .ok_or_else(|| anyhow!("Cannot be expressed as u64: {}", num))
                .map(|v| Type::UnsignedInteger(v)),
            (ValueType::Text, JsonValue::String(s)) => Ok(Type::Text(s.to_string())),
            (ValueType::Text, JsonValue::Bool(b)) => Ok(Type::Text(b.to_string())),
            (ValueType::Text, JsonValue::Number(num)) => Ok(Type::Text(num.to_string())),
            (other_type, other_value) => Err(anyhow!("Unable to parse self from JSON; need type {} but got '{}'", other_type, other_value)),
        }
    }
}
