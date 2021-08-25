use log::LevelFilter;
use serde::Deserialize;
use serde_yaml::from_str;
use std::io::Read;
use std::{collections::HashMap, fs::File, path::Path, time::Duration};

use crate::value::ValueType;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", untagged)]
pub enum MqttAuth {
    #[serde(rename_all = "camelCase")]
    UserPass { username: String, password: String },
    #[serde(rename_all = "camelCase")]
    Certificate {
        cert_file: String,
        private_key_file: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub client_id: String,
    pub auth: Option<MqttAuth>,
    pub ca_file: Option<String>,
    pub connect_timeout: Option<Duration>,
    pub keep_alive: Option<Duration>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TagValue {
    pub r#type: ValueType,
    pub value: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserAuth {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InfluxDBConfig {
    pub url: String,
    pub auth: Option<UserAuth>,
    pub db_name: String,
    pub measurement: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mapping {
    pub topic: String,
    pub field_name: String,
    pub value_type: ValueType,
    pub tags: HashMap<String, TagValue>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub log_level: Option<LevelFilter>,
    pub mqtt: MqttConfig,
    pub database: InfluxDBConfig,
    pub mappings: Vec<Mapping>,
}

impl Config {
    pub fn parse<P: AsRef<Path>>(filename: P) -> Result<Config, String> {
        let mut f = File::open(filename).map_err(|err| err.to_string())?;
        let mut contents = String::new();
        f.read_to_string(&mut contents)
            .map_err(|err| err.to_string())?;
        let config: Config = from_str(&contents).map_err(|err| err.to_string())?;
        Ok(config)
    }
}
