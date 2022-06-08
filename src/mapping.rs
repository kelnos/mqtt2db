use influxdb::Type;
use jsonpath::Selector;
use std::{convert::TryFrom, fmt};

use crate::config::{Mapping as ConfigMapping, Payload as ConfigPayload, TagValue as ConfigTagValue};
use crate::interpolate::{InterpolatedName, InterpolatedNamePart};
use crate::value::{ToInfluxType, ValueType};

#[derive(Clone, Debug, PartialEq)]
pub enum TopicLevel {
    Literal(String),
    SingleWildcard,
    MultiWildcard,
}

impl TryFrom<&str> for TopicLevel {
    type Error = anyhow::Error;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "+" => Ok(TopicLevel::SingleWildcard),
            "#" => Ok(TopicLevel::MultiWildcard),
            s if s.contains("+") || s.contains("#") => {
                Err(anyhow!("Topic level '{}' cannot contain '+' or '#'", s))
            }
            s => Ok(TopicLevel::Literal(s.to_string())),
        }
    }
}

#[derive(Debug)]
pub enum TagValue {
    InterpolatedStr(InterpolatedName),
    Literal(Type),
}

impl TryFrom<&ConfigTagValue> for TagValue {
    type Error = anyhow::Error;
    fn try_from(tag_value: &ConfigTagValue) -> Result<Self, Self::Error> {
        match tag_value.r#type {
            ValueType::Text => {
                let interp = InterpolatedName::try_from(tag_value.value.as_str())?;
                match interp.parts.get(0) {
                    Some(InterpolatedNamePart::Literal(literal)) if interp.parts.len() == 1 => {
                        Ok(TagValue::Literal(Type::Text(literal.clone())))
                    }
                    _ => Ok(TagValue::InterpolatedStr(interp)),
                }
            }
            other => tag_value.value.to_influx_type(other).map(TagValue::Literal),
        }
    }
}

pub enum Payload {
    Raw,
    Json {
        value_field_selector: Selector,
        timestamp_field_selector: Option<Selector>,
    },
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Payload::*;
        match self {
            Raw => write!(f, "Raw"),
            Json { .. } => write!(f, "Json {{ ... }}"),
        }
    }
}

#[derive(Debug)]
pub struct Mapping {
    pub topic: Vec<TopicLevel>,
    pub payload: Payload,
    pub field_name: InterpolatedName,
    pub value_type: ValueType,
    pub tags: Vec<(String, TagValue)>,
}

impl TryFrom<&ConfigMapping> for Mapping {
    type Error = anyhow::Error;
    fn try_from(mapping: &ConfigMapping) -> Result<Self, Self::Error> {
        let topic = mapping
            .topic
            .split("/")
            .map(|level| TopicLevel::try_from(level))
            .collect::<anyhow::Result<Vec<TopicLevel>>>()?;
        let pre_multi_levels: Vec<&TopicLevel> = topic
            .iter()
            .take_while(|level| **level != TopicLevel::MultiWildcard)
            .collect();
        if pre_multi_levels.len() < topic.len() - 1 {
            Err(anyhow!(
                "Topic '{}' has '#' wildcard before last topic level",
                mapping.topic
            ))?;
        }

        let max_interp_ref = topic
            .iter()
            .filter(|level| **level == TopicLevel::SingleWildcard)
            .count();

        let field_name = match InterpolatedName::try_from(mapping.field_name.as_str()) {
            Ok(name) if find_max_ref(&name) > max_interp_ref => Err(anyhow!(
                "Topic '{}' has field name '{}' which has invalid references",
                mapping.topic, mapping.field_name
            )),
            Ok(name) => Ok(name),
            Err(err) => Err(err),
        }?;

        let payload = match &mapping.payload {
            None => Payload::Raw,
            Some(ConfigPayload::Json { value_field_path, timestamp_field_path }) => {
                let value_field_selector = Selector::new(&value_field_path)
                    .map_err(|err| anyhow!("Value field path '{}' is invalid: {}'", value_field_path, err))?;
                let timestamp_field_selector = timestamp_field_path.as_ref()
                    .map(|path| Selector::new(path)
                        .map_err(|err| anyhow!("Timestamp field path '{}' is invalid: {}'", path, err))
                    )
                    .transpose()?;
                Payload::Json {
                    value_field_selector,
                    timestamp_field_selector,
                }
            }
        };

        let tags = mapping
            .tags
            .iter()
            .map(|tag| match TagValue::try_from(tag.1) {
                Ok(TagValue::InterpolatedStr(ref name)) if find_max_ref(name) > max_interp_ref => {
                    Err(anyhow!(
                        "Topic '{}' has tag value '{:?}' which has invalid references",
                        mapping.topic, tag.1
                    ))
                }
                Ok(value) => Ok((tag.0.clone(), value)),
                Err(err) => Err(err),
            })
            .collect::<anyhow::Result<Vec<(String, TagValue)>>>()?;

        Ok(Mapping {
            topic,
            payload,
            field_name,
            value_type: mapping.value_type,
            tags,
        })
    }
}

fn find_max_ref(name: &InterpolatedName) -> usize {
    name.parts.iter().fold(0, |max_ref, part| match part {
        InterpolatedNamePart::Reference(num) if *num > max_ref => *num,
        _ => max_ref,
    })
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn mapping_parsing() -> anyhow::Result<()> {
        use TopicLevel::*;

        fn mk_cfg_mapping(topic: &str) -> ConfigMapping {
            ConfigMapping {
                topic: topic.to_string(),
                payload: None,
                field_name: "".to_string(),
                value_type: ValueType::Text,
                tags: HashMap::new(),
            }
        }

        assert_eq!(
            vec![Literal("foo".to_string()), Literal("bar".to_string())],
            Mapping::try_from(&mk_cfg_mapping("foo/bar"))?.topic
        );

        assert_eq!(
            vec![
                Literal("".to_string()),
                Literal("foo".to_string()),
                Literal("bar".to_string())
            ],
            Mapping::try_from(&mk_cfg_mapping("/foo/bar"))?.topic
        );

        assert_eq!(
            vec![
                Literal("foo".to_string()),
                Literal("bar".to_string()),
                Literal("".to_string())
            ],
            Mapping::try_from(&mk_cfg_mapping("foo/bar/"))?.topic
        );

        assert_eq!(
            vec![
                Literal("foo".to_string()),
                Literal("bar".to_string()),
                MultiWildcard
            ],
            Mapping::try_from(&mk_cfg_mapping("foo/bar/#"))?.topic
        );

        assert_eq!(
            vec![
                Literal("foo".to_string()),
                SingleWildcard,
                Literal("bar".to_string())
            ],
            Mapping::try_from(&mk_cfg_mapping("foo/+/bar"))?.topic
        );

        assert_eq!(
            vec![
                Literal("foo".to_string()),
                SingleWildcard,
                Literal("bar".to_string()),
                MultiWildcard
            ],
            Mapping::try_from(&mk_cfg_mapping("foo/+/bar/#"))?.topic
        );

        assert!(Mapping::try_from(&mk_cfg_mapping("foo/#/bar")).is_err());
        assert!(Mapping::try_from(&mk_cfg_mapping("foo/bar#")).is_err());
        assert!(Mapping::try_from(&mk_cfg_mapping("foo/bar+baz/quux")).is_err());
        assert!(Mapping::try_from(&mk_cfg_mapping("foo/bar#baz/quux")).is_err());

        Ok(())
    }
}
