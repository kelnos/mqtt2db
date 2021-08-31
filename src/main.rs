#[macro_use]
extern crate log;

use config::{Config, InfluxDBConfig, MqttAuth, MqttConfig, UserAuth};
use futures::TryFutureExt;
use influxdb::InfluxDbWriteable;
use influxdb::{Client as InfluxClient, Timestamp, Type};
use log::LevelFilter;
use mapping::{Mapping, Payload, TagValue, TopicLevel};
use rumqttc::{
    AsyncClient as MqttAsyncClient, Event, EventLoop as MqttEventLoop, Key, MqttOptions, Packet,
    Publish, QoS, SubscribeFilter, TlsConfiguration, Transport,
};
use serde_json::Value as JsonValue;
use std::convert::TryFrom;
use std::env;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use value::ToInfluxType;

mod config;
mod interpolate;
mod mapping;
mod value;

struct Database {
    client: InfluxClient,
    measurement: String,
}

async fn init_mqtt(config: &MqttConfig) -> Result<(MqttAsyncClient, MqttEventLoop), String> {
    async fn file_to_bytevec<P: AsRef<Path>>(file: P) -> Result<Vec<u8>, String> {
        let mut f = File::open(&file)
            .map_err(|err| {
                format!(
                    "Unable to open {}: {}",
                    file.as_ref().to_string_lossy(),
                    err
                )
            })
            .await?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)
            .map_err(|err| {
                format!(
                    "Unable to read {}: {}",
                    file.as_ref().to_string_lossy(),
                    err
                )
            })
            .await?;
        Ok(buf)
    }

    let mut options = MqttOptions::new(config.client_id.clone(), config.host.clone(), config.port);
    if let Some(connect_timeout) = config.connect_timeout {
        options.set_connection_timeout(connect_timeout.as_secs());
    }
    if let Some(keep_alive) = config.keep_alive {
        let keep_alive_secs = u16::try_from(keep_alive.as_secs())
            .map_err(|_| "Keep alive time must be between 0 and 65535".to_string())?;
        options.set_keep_alive(keep_alive_secs);
    }
    if let Some(ca_file) = &config.ca_file {
        let ca = file_to_bytevec(ca_file).await?;
        options.set_transport(Transport::Tls(TlsConfiguration::Simple {
            ca,
            alpn: None,
            client_auth: if let Some(MqttAuth::Certificate {
                cert_file,
                private_key_file,
            }) = &config.auth
            {
                let cert = file_to_bytevec(cert_file).await?;
                let private_key = file_to_bytevec(private_key_file).await?;
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

fn init_db(config: &InfluxDBConfig) -> Result<Database, String> {
    let mut client = InfluxClient::new(config.url.clone(), config.db_name.clone());
    if let Some(UserAuth { username, password }) = &config.auth {
        client = client.with_auth(username, password);
    }
    Ok(Database {
        client,
        measurement: config.measurement.clone(),
    })
}

async fn init_subscriptions(
    mqtt_client: &mut MqttAsyncClient,
    topics: &Vec<&String>,
) -> Result<(), String> {
    let topics: Vec<SubscribeFilter> = topics
        .iter()
        .map(|topic| {
            info!("Subscribing to topic '{}'", topic);
            SubscribeFilter::new((*topic).clone(), QoS::AtLeastOnce)
        })
        .collect();
    mqtt_client
        .subscribe_many(topics)
        .await
        .map_err(|err| err.to_string())?;
    Ok(())
}

async fn handle_publish(
    publish: &Publish,
    mapping: Arc<Mapping>,
    database: Arc<Database>,
) -> Result<(), String> {
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
        .map_err(|err| format!("Invalid payload value: {}", err))?;
    let (influx_value, timestamp) = match &mapping.payload {
        Payload::Raw => (payload.to_influx_type(mapping.value_type)?, None),
        Payload::Json { value_field_selector, timestamp_field_selector } => {
            let payload_root: JsonValue = serde_json::from_str(&payload)
                .map_err(|err| format!("Failed to parse payload as JSON: {}", err))?;
            let influx_value = value_field_selector
                .find(&payload_root)
                .next()
                .ok_or_else(|| format!("Couldn't find value in payload on topic {}", publish.topic))
                .and_then(|value| value.to_influx_type(mapping.value_type))?;
            let timestamp = timestamp_field_selector
                .as_ref()
                .map(|selector| selector
                    .find(&payload_root)
                    .next()
                    .ok_or_else(|| format!("Couldn't find timestamp in payload on topic {}", publish.topic))
                    .and_then(|ts_value| ts_value
                        .as_u64()
                        .map(|ts| ts as u128)
                        .ok_or_else(|| format!("'{}' cannot be converted to a timestamp", ts_value))
                    )
                )
                .transpose()?;
            (influx_value, timestamp)
        },
    };

    let timestamp = timestamp.unwrap_or_else(|| SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
    );
    let mut query = Timestamp::Milliseconds(timestamp)
        .into_query(&database.measurement)
        .add_field(&field_name, influx_value);
    for tag in mapping.tags.iter() {
        let value = match &tag.1 {
            TagValue::Literal(v) => v.clone(),
            TagValue::InterpolatedStr(interp) => Type::Text(interp.interpolate(&reference_values)?),
        };
        query = query.add_tag(&tag.0, value);
    }

    use tokio_compat_02::FutureExt;
    database
        .client
        .query(&query)
        .compat()
        .await
        .map_err(|err| format!("Failed to write to DB: {}", err))?;
    debug!("wrote to influx: {:?}", query);

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
    mappings: &Vec<Arc<Mapping>>,
    database: Arc<Database>,
) {
    loop {
        match event_loop.poll().await {
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                if let Some(mapping) = find_mapping(&mappings, &publish.topic) {
                    let mapping = Arc::clone(mapping);
                    let database = Arc::clone(&database);
                    tokio::spawn(async move {
                        if let Err(err) = handle_publish(&publish, mapping, database).await {
                            warn!("{}", err);
                        }
                    });
                } else {
                    warn!("Topic {} not found in mappings", publish.topic);
                }
            }
            Ok(Event::Incoming(_event)) => (),
            Ok(Event::Outgoing(_event)) => (),
            Err(err) => warn!("Error from MQTT loop: {:#?}", err),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let config_filename = env::args()
        .nth(1)
        .ok_or_else(|| "Missing argument 'config filename'")?;
    let config = Config::parse(&config_filename)?;

    let logger_env = env_logger::Env::new()
        .filter("MQTT2DB_LOG")
        .write_style("MQTT2DB_LOG_STYLE");
    let mut logger_builder = env_logger::Builder::from_env(logger_env);
    logger_builder
        .filter_level(config.log_level.unwrap_or(LevelFilter::Info))
        .init();

    let mappings: Vec<Arc<Mapping>> = config
        .mappings
        .iter()
        .map(Mapping::try_from)
        .collect::<Result<Vec<Mapping>, String>>()?
        .into_iter()
        .map(Arc::new)
        .collect();

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

    let database = Arc::new(init_db(&config.database)?);

    run_event_loop(mqtt_event_loop, &mappings, database).await;

    Ok(())
}
