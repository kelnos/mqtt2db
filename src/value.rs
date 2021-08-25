use influxdb::Type;
use serde::Deserialize;

#[derive(Copy, Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ValueType {
    Boolean,
    Float,
    SignedInteger,
    UnsignedInteger,
    Text,
}

impl ValueType {
    pub fn parse(self, value: &String) -> Result<Type, String> {
        match self {
            ValueType::Boolean if value == "true" => Ok(Type::Boolean(true)),
            ValueType::Boolean if value == "false" => Ok(Type::Boolean(false)),
            ValueType::Boolean => Err(format!("Value '{}' is not a valid boolean", value)),
            ValueType::Float => value
                .parse::<f64>()
                .map(|v| Type::Float(v))
                .map_err(|err| err.to_string()),
            ValueType::SignedInteger => value
                .parse::<i64>()
                .map(|v| Type::SignedInteger(v))
                .map_err(|err| err.to_string()),
            ValueType::UnsignedInteger => value
                .parse::<u64>()
                .map(|v| Type::UnsignedInteger(v))
                .map_err(|err| err.to_string()),
            ValueType::Text => Ok(Type::Text(value.clone())),
        }
    }
}
