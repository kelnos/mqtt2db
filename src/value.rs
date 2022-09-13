// mqtt2db -- subscries to MQTT topics and writes to a database
// Copyright (C) 2021-2022 Brian Tarricone <brian@tarricone.org>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

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
