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

#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate log;

use config::{Config, Database as ConfigDatabase, MqttAuth, MqttConfig, UserAuth};
use influxdb::InfluxDbWriteable;
use influxdb::{Client as InfluxClient, Timestamp, Type};
use mapping::{Mapping, Payload, TagValue, TopicLevel};
use rumqttc::{
    AsyncClient as MqttAsyncClient, Event, EventLoop as MqttEventLoop, Key, MqttOptions, Packet,
    Publish, QoS, SubscribeFilter, TlsConfiguration, Transport,
};
use serde_json::Value as JsonValue;
use std::convert::TryFrom;
use std::env;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;
use value::ToInfluxType;

mod config;
mod interpolate;
mod mapping;
mod value;

struct Database {
    client: InfluxClient,
    measurement: String,
}

async fn init_mqtt(config: &MqttConfig) -> anyhow::Result<(MqttAsyncClient, MqttEventLoop)> {
    let mut options = MqttOptions::new(&config.client_id, &config.host, config.port);
    if let Some(connect_timeout) = config.connect_timeout {
        options.set_connection_timeout(connect_timeout.as_secs());
    }
    if let Some(keep_alive) = config.keep_alive {
        options.set_keep_alive(keep_alive);
    }
    if let Some(ca_file) = &config.ca_file {
        let ca = fs::read(ca_file).await?;
        options.set_transport(Transport::Tls(TlsConfiguration::Simple {
            ca,
            alpn: None,
            client_auth: if let Some(MqttAuth::Certificate {
                cert_file,
                private_key_file,
            }) = &config.auth
            {
                let cert = fs::read(cert_file).await?;
                let private_key = fs::read(private_key_file).await?;
                Some((cert, Key::RSA(private_key)))
            } else {
                None
            },
        }));
    }

    if let Some(MqttAuth::UserPass { username, password }) = &config.auth {
        options.set_credentials(username, password);
    }

    Ok(MqttAsyncClient::new(options, 100))
}

fn init_db(config: &ConfigDatabase) -> anyhow::Result<Database> {
    match config {
        ConfigDatabase::Influxdb { url, auth, db_name, measurement } => {
            let mut client = InfluxClient::new(url, db_name);
            if let Some(UserAuth { username, password }) = auth {
                client = client.with_auth(username, password);
            }
            Ok(Database {
                client,
                measurement: measurement.clone(),
            })
        }
    }
}

async fn init_subscriptions(
    mqtt_client: &mut MqttAsyncClient,
    topics: &Vec<&String>,
) -> anyhow::Result<()> {
    let topics: Vec<SubscribeFilter> = topics
        .iter()
        .map(|topic| {
            info!("Subscribing to topic '{}'", topic);
            SubscribeFilter::new((*topic).clone(), QoS::AtLeastOnce)
        })
        .collect();
    mqtt_client
        .subscribe_many(topics)
        .await?;
    Ok(())
}

async fn handle_publish(
    publish: &Publish,
    mapping: Arc<Mapping>,
    databases: Arc<Vec<Database>>,
) -> anyhow::Result<()> {
    debug!("Got publish: {:?}; {:?}", publish, publish.payload);

    let reference_values = publish
        .topic
        .split("/")
        .zip(mapping.topic.iter())
        .flat_map(|pair| match pair.1 {
            TopicLevel::SingleWildcard => Some(pair.0),
            _ => None,
        })
        .collect::<Vec<&str>>();
    let field_name = mapping.field_name.interpolate(&reference_values)?;

    let payload = String::from_utf8(Vec::from(publish.payload.as_ref()))
        .map_err(|err| anyhow!("Invalid payload value: {}", err))?;
    let (influx_value, timestamp) = match &mapping.payload {
        Payload::Raw => (payload.to_influx_type(mapping.value_type)?, None),
        Payload::Json { value_field_selector, timestamp_field_selector } => {
            let payload_root: JsonValue = serde_json::from_str(&payload)
                .map_err(|err| anyhow!("Failed to parse payload as JSON: {}", err))?;
            let influx_value = value_field_selector
                .find(&payload_root)
                .next()
                .ok_or_else(|| anyhow!("Couldn't find value in payload on topic {}", publish.topic))
                .and_then(|value| value.to_influx_type(mapping.value_type))?;
            let timestamp = timestamp_field_selector
                .as_ref()
                .map(|selector| selector
                    .find(&payload_root)
                    .next()
                    .ok_or_else(|| anyhow!("Couldn't find timestamp in payload on topic {}", publish.topic))
                    .and_then(|ts_value| ts_value
                        .as_u64()
                        .map(|ts| ts as u128)
                        .ok_or_else(|| anyhow!("'{}' cannot be converted to a timestamp", ts_value))
                    )
                )
                .transpose()?;
            (influx_value, timestamp)
        },
    };

    let timestamp = timestamp.unwrap_or_else(|| SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
    );

    for database in databases.iter() {
        let mut query = Timestamp::Nanoseconds(timestamp)
            .into_query(&database.measurement)
            .add_field(&field_name, influx_value.clone());
        for tag in mapping.tags.iter() {
            let value = match &tag.1 {
                TagValue::Literal(v) => v.clone(),
                TagValue::InterpolatedStr(interp) => Type::Text(interp.interpolate(&reference_values)?),
            };
            query = query.add_tag(&tag.0, value);
        }

        database
            .client
            .query(&query)
            .await
            .map_err(|err| anyhow!("Failed to write to DB: {}", err))?;
        debug!("wrote to influx: {:?}", query);
    }

    Ok(())
}

fn find_mapping<'a>(mappings: &'a Vec<Arc<Mapping>>, topic: &String) -> Option<&'a Arc<Mapping>> {
    let levels: Vec<&str> = topic.split("/").collect();
    mappings.iter().find(|mapping| {
        let mut iter = levels.iter();
        for expected_level in mapping.topic.iter() {
            let maybe_cur_level = iter.next();
            match (expected_level, maybe_cur_level) {
                (TopicLevel::SingleWildcard, Some(_)) => (), // current level exists and anything matches
                (TopicLevel::MultiWildcard, _) => return true, // rest of topic, if any, will match no matter what
                (TopicLevel::Literal(expected_literal), Some(cur_level))
                    if expected_literal == cur_level =>
                {
                    ()
                } // current level matches
                _ => return false, // current level doesn't match or doesn't exist
            }
        }
        iter.next().is_none() // only matches if we consumed all topic levels
    })
}

async fn run_event_loop(
    mut event_loop: MqttEventLoop,
    mappings: Vec<Mapping>,
    databases: Vec<Database>,
) {
    let mappings = mappings.into_iter().map(Arc::new).collect();
    let databases = Arc::new(databases);

    loop {
        match event_loop.poll().await {
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                if let Some(mapping) = find_mapping(&mappings, &publish.topic) {
                    let mapping = Arc::clone(mapping);
                    let databases = Arc::clone(&databases);
                    tokio::spawn(async move {
                        if let Err(err) = handle_publish(&publish, mapping, databases).await {
                            warn!("{}", err);
                        }
                    });
                } else {
                    warn!("Topic {} not found in mappings", publish.topic);
                }
            }
            Ok(_) => (),
            Err(err) => warn!("Error from MQTT loop: {:#?}", err),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_filename = env::args()
        .nth(1)
        .ok_or_else(|| anyhow!("Missing argument 'config filename'"))?;
    let config = Config::parse(&config_filename)?;

    let logger_env = env_logger::Env::new()
        .filter("MQTT2DB_LOG")
        .write_style("MQTT2DB_LOG_STYLE");
    let mut logger_builder = env_logger::Builder::from_env(logger_env);
    if let Some(log_level) = config.log_level {
        logger_builder.filter_level(log_level);
    }
    logger_builder.init();

    let mappings: Vec<Mapping> = config
        .mappings
        .iter()
        .map(Mapping::try_from)
        .collect::<anyhow::Result<Vec<Mapping>>>()?;

    let (mut mqtt_client, mqtt_event_loop) = init_mqtt(&config.mqtt).await?;
    init_subscriptions(
        &mut mqtt_client,
        &config
            .mappings
            .iter()
            .map(|mapping| &mapping.topic)
            .collect(),
    )
    .await?;

    let databases = config.databases
        .iter()
        .map(init_db)
        .collect::<anyhow::Result<Vec<Database>>>()?;

    run_event_loop(mqtt_event_loop, mappings, databases).await;

    Ok(())
}
