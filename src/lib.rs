//! Kafka para o OpenIDEA.
//!
//! O WASM do plugin não tem sockets TCP: as operações são executadas pelos
//! scripts da distribuição Apache Kafka (`process:<programa>`), que o próprio
//! plugin pode baixar e instalar em `plugin-dir`. A UI é um conjunto de tool
//! windows (`[[contributes.views]]`): explorador em árvore com painel de
//! detalhes, tabelas de tópicos/brokers/grupos, formulário de conexões e um
//! visualizador de mensagens em página HTML sandboxed.

mod backend;
mod config;
mod formats;
mod i18n;
mod messages;
mod model;

use backend::{BackendStatus, ConsumeOptions, PublishMessage};
use config::{Connection, ConsumePrefs};
use messages::MessageBuffer;
use model::{ClusterInfo, GroupLag, Partition, Topic};
use openidea_plugin::details::{Details, Pane, Tab};
use openidea_plugin::host;
use openidea_plugin::views::{
    Body, Button, Field, FieldValue, Table, TableSort, TreeItem, View, ViewEvent, Viewport, Views,
};
use openidea_plugin::{
    Error, Event, EventKind, Invocation, NotificationLevel, PickItem, Plugin, StatusItem,
    register_plugin,
};
use serde_json::{Value, json};
use std::collections::BTreeSet;

const EXPLORER: &str = "explorer";
const TOPICS: &str = "topics";
const MESSAGES: &str = "messages";
const CONNECTIONS: &str = "connections";
const BROKERS: &str = "brokers";
const GROUPS: &str = "groups";
const INSTALL: &str = "install";
const ALL_VIEWS: [&str; 7] = [
    EXPLORER,
    TOPICS,
    MESSAGES,
    CONNECTIONS,
    BROKERS,
    GROUPS,
    INSTALL,
];
const PAGE: &str = include_str!("page.html");
/// The status-bar contribution declared in `plugin.toml`; the plugin replaces
/// its label/tooltip with the outcome of the last command.
const STATUS_ID: &str = "kafka";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
enum Selection {
    #[default]
    None,
    Connection(String),
    Topic(String),
    Partition(String, i32),
    Broker(i32),
    Group(String),
}

#[derive(Default)]
pub struct Kafka {
    topics: Vec<Topic>,
    topic_names: Vec<String>,
    groups: Vec<String>,
    group_lags: Vec<GroupLag>,
    cluster: ClusterInfo,
    notes: Vec<String>,
    selected: Selection,
    selected_topic: Option<Topic>,
    messages: MessageBuffer,
    prefs: ConsumePrefs,
    /// Rascunho do formulário de conexões (`None`: sem edição).
    draft: Option<Connection>,
    versions: Vec<String>,
    backend: Option<BackendStatus>,
    status: String,
    sort: Option<TableSort>,
    expanded: BTreeSet<String>,
}

impl Kafka {
    // ------------------------------------------------------------- estado

    fn reload(&mut self) -> Result<(), Error> {
        self.notes.clear();
        let Some(c) = config::active() else {
            self.topics.clear();
            self.topic_names.clear();
            self.groups.clear();
            self.group_lags.clear();
            self.cluster = ClusterInfo::default();
            self.status = i18n::t("No active connection. Add a cluster in Connections.");
            return Ok(());
        };
        self.topic_names = backend::list_topics(&c)?;
        self.topics = backend::describe_topics(&c, None)?;
        self.groups = match backend::list_groups(&c) {
            Ok(groups) => groups,
            Err(e) => {
                self.notes.push(e.to_string());
                Vec::new()
            }
        };
        let (info, mut notes) = backend::cluster_info(&c);
        self.cluster = info;
        self.notes.append(&mut notes);
        self.status = i18n::tf(
            "{name} · {count} topics · {brokers} brokers · {groups} groups",
            &[
                ("name", c.name.as_str()),
                ("count", &self.topic_names.len().to_string()),
                ("brokers", &self.cluster.brokers.len().to_string()),
                ("groups", &self.groups.len().to_string()),
            ],
        );
        Ok(())
    }

    fn active(&self) -> Result<Connection, Error> {
        config::active().ok_or_else(|| {
            i18n::t("No connections configured. Use 'Kafka: New Connection' to get started.").into()
        })
    }

    /// Testa um perfil (salvo ou rascunho) e devolve o texto do resultado.
    fn tested_connection_text(&self, profile: &Connection) -> Result<String, Error> {
        let count = backend::test_connection(profile)?;
        Ok(i18n::tf(
            "Connection OK: {count} topics at {servers}.",
            &[
                ("count", &count.to_string()),
                ("servers", &profile.bootstrap_servers),
            ],
        ))
    }

    fn ensure_topics(&mut self) -> Result<(), Error> {
        if self.topic_names.is_empty() {
            self.reload()?;
        }
        Ok(())
    }

    fn inspect_topic(&mut self, c: &Connection, name: &str) -> Result<Topic, Error> {
        let mut topic = backend::describe_topics(c, Some(name))?
            .into_iter()
            .find(|t| t.name == name)
            .unwrap_or_else(|| Topic {
                name: name.into(),
                ..Topic::default()
            });
        for note in backend::fill_offsets(c, &mut topic) {
            self.notes.push(note);
        }
        self.selected_topic = Some(topic.clone());
        Ok(topic)
    }

    fn consume(&mut self, c: &Connection, opts: &ConsumeOptions) -> Result<usize, Error> {
        let records = backend::consume(c, opts)?;
        let count = records.len();
        self.messages.append(&c.id, &opts.topic, records);
        self.prefs.topic = opts.topic.clone();
        self.prefs.partition = opts.partition.map(|p| p.to_string()).unwrap_or_default();
        self.prefs.start = opts.start.clone();
        self.prefs.offset = opts.offset.clone();
        self.prefs.limit = opts.limit;
        self.prefs.save();
        Ok(count)
    }

    fn refresh_all() {
        for view in ALL_VIEWS {
            host::refresh_view(view);
        }
    }

    fn note(&mut self, message: impl Into<String>) {
        self.status = message.into();
    }

    /// Persistent feedback in the status bar. Notifications can be hidden by
    /// the IDE; the chip stays until the next command.
    fn report_result(&mut self, result: &Result<Option<String>, Error>) {
        let (mark, tooltip, show_in_view) = match result {
            Ok(Some(text)) => ("✓", text.clone(), true),
            Ok(None) => ("•", i18n::t("Kafka ready."), false),
            Err(Error::Cancelled) => ("•", i18n::t("Kafka: cancelled."), true),
            Err(e) => ("✗", e.to_string(), true),
        };
        host::set_status_item(
            STATUS_ID,
            &StatusItem {
                text: format!("Kafka {mark}"),
                tooltip: Some(tooltip.clone()),
                command: Some("open".into()),
            },
        );
        if show_in_view {
            self.note(tooltip);
        }
    }

    /// Every command goes through here so view buttons and palette commands
    /// get the same success/failure chip.
    fn run(
        &mut self,
        id: &str,
        ctx: &Invocation,
        arg: Option<&str>,
    ) -> Result<Option<String>, Error> {
        let result = self.command(id, ctx, arg);
        self.report_result(&result);
        result
    }

    // ------------------------------------------------------------- dados auxiliares

    fn choose_topic(&mut self, arg: Option<&str>) -> Result<String, Error> {
        if let Some(name) = arg.filter(|s| !s.is_empty()) {
            return Ok(name.into());
        }
        self.ensure_topics()?;
        pick_one(
            &i18n::t("Kafka: choose a topic"),
            &self.topic_names,
            &i18n::t("No topics in the cluster."),
        )
    }

    fn choose_connection(&self, arg: Option<&str>) -> Result<String, Error> {
        if let Some(id) = arg.filter(|s| !s.is_empty()) {
            return Ok(id.into());
        }
        let profiles = config::load_profiles();
        pick_one(
            &i18n::t("Kafka: choose a connection"),
            &profiles.iter().map(|p| p.id.clone()).collect::<Vec<_>>(),
            &i18n::t("No connections configured."),
        )
    }

    fn consume_options(&self, topic: &str, limit: u32) -> ConsumeOptions {
        ConsumeOptions {
            topic: topic.into(),
            partition: self.prefs.partition.parse().ok(),
            start: self.prefs.start.clone(),
            offset: self.prefs.offset.clone(),
            limit,
            timeout_ms: config::timeout_ms(),
            deserializer_format: config::formats_for(topic).deserializer,
            live: false,
        }
    }

    /// Save the formats sent by the HTML page next to the chosen topic.
    fn remember_formats(&self, payload: &Value) {
        let topic = payload["topic"]
            .as_str()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(&self.prefs.topic);
        for (field, kind) in [
            ("deserializer", "deserializer"),
            ("serializer", "serializer"),
        ] {
            if let Some(value) = payload[field].as_str() {
                let _ = config::set_format(topic, kind, value);
            }
        }
    }

    fn messages_state(&self) -> String {
        let mut value: Value =
            serde_json::from_str(&self.messages.to_json(&self.prefs, &self.status))
                .unwrap_or_else(|_| json!({}));
        let profiles = config::load_profiles();
        value["connections"] = json!(
            profiles
                .iter()
                .map(|p| json!({"id": p.id, "name": p.name, "bootstrap": p.bootstrap_servers}))
                .collect::<Vec<_>>()
        );
        value["active"] = json!(config::active_id());
        value["topics"] = json!(self.topic_names);
        value["formats"] = json!(formats::NAMES);
        value["topicFormats"] = json!(config::load_formats());
        value["installed"] = json!(config::install_info().map(|i| i.bin));
        // The IDE does not translate HTML bodies: ship the same catalog the
        // IDE uses for the manifest/views so the page can translate itself.
        value["i18n"] = if i18n::portuguese() {
            serde_json::from_str::<Value>(i18n::catalog_json()).unwrap_or_else(|_| json!({}))
        } else {
            json!({})
        };
        value.to_string()
    }

    fn post_state(&self) {
        let _ = host::post_view_message(MESSAGES, &self.messages_state());
    }

    // ------------------------------------------------------------- comandos

    fn command(
        &mut self,
        id: &str,
        _ctx: &Invocation,
        arg: Option<&str>,
    ) -> Result<Option<String>, Error> {
        match id {
            "open" => {
                host::reveal_view(EXPLORER)?;
                Ok(None)
            }
            "refresh" => {
                self.reload()?;
                Self::refresh_all();
                Ok(Some(self.status.clone()))
            }
            "connections" => {
                host::reveal_view(CONNECTIONS)?;
                Ok(None)
            }
            "connection-new" => {
                self.draft = Some(Connection::new("", &i18n::t("New cluster")));
                host::reveal_view(CONNECTIONS)?;
                host::refresh_view(CONNECTIONS);
                Ok(None)
            }
            "connection-edit" => {
                let id = self.choose_connection(arg)?;
                let profile = config::by_id(&id)
                    .ok_or_else(|| i18n::tf("Connection {id} not found.", &[("id", &id)]))?;
                self.draft = Some(profile);
                host::reveal_view(CONNECTIONS)?;
                host::refresh_view(CONNECTIONS);
                Ok(None)
            }
            "connection-test" => {
                let id = self.choose_connection(arg)?;
                let profile = config::by_id(&id)
                    .ok_or_else(|| i18n::tf("Connection {id} not found.", &[("id", &id)]))?;
                let text = self.tested_connection_text(&profile)?;
                host::notify(NotificationLevel::Info, "Kafka", &text);
                Ok(Some(text))
            }
            "connection-test-draft" => {
                let mut profile = self.draft.clone().ok_or_else(|| {
                    Error::from(i18n::t(
                        "No connection is being edited. Use 'Kafka: New Connection.'",
                    ))
                })?;
                if profile.id.is_empty() {
                    profile.id = config::new_id(&profile.name);
                }
                let text = self.tested_connection_text(&profile)?;
                host::notify(NotificationLevel::Info, "Kafka", &text);
                Ok(Some(text))
            }
            "connection-delete" => {
                let id = self.choose_connection(arg)?;
                let profile = config::by_id(&id)
                    .ok_or_else(|| i18n::tf("Connection {id} not found.", &[("id", &id)]))?;
                let body = i18n::tf(
                    "Delete the connection {name} ({servers})?",
                    &[
                        ("name", &profile.name),
                        ("servers", &profile.bootstrap_servers),
                    ],
                );
                if !host::confirm(&i18n::t("Kafka: delete connection"), &body)? {
                    return Err(Error::Cancelled);
                }
                config::remove(&id)?;
                self.draft = None;
                let _ = self.reload();
                Self::refresh_all();
                Ok(Some(i18n::tf(
                    "Connection {name} deleted.",
                    &[("name", &profile.name)],
                )))
            }
            "connection-activate" => {
                let id = self.choose_connection(arg)?;
                config::set_active(&id);
                let _ = self.reload();
                Self::refresh_all();
                Ok(Some(i18n::tf("Active connection: {id}", &[("id", &id)])))
            }
            "connection-save" => {
                let mut profile = self.draft.clone().ok_or_else(|| {
                    i18n::t("No connection is being edited. Use 'Kafka: New Connection'.")
                })?;
                if profile.id.is_empty() {
                    profile.id = config::new_id(&profile.name);
                }
                // Testa antes de salvar, como pedido nos requisitos.
                let count = backend::test_connection(&profile)?;
                config::upsert(profile.clone())?;
                config::set_active(&profile.id);
                self.draft = None;
                let text = i18n::tf(
                    "Connection {name} saved and activated ({count} topics found).",
                    &[("name", &profile.name), ("count", &count.to_string())],
                );
                let _ = self.reload();
                // `reload` sets the cluster summary; the last action wins in
                // the view so the save result is actually visible.
                self.note(text.clone());
                Self::refresh_all();
                host::notify(NotificationLevel::Info, "Kafka", &text);
                Ok(Some(text))
            }
            "install-backend" => {
                let version = if let Some(v) = arg.filter(|s| !s.is_empty()) {
                    v.to_owned()
                } else {
                    self.versions = backend::available_versions();
                    pick_one(
                        &i18n::t("Kafka: version to install"),
                        &self.versions,
                        &i18n::t("Version catalog unavailable."),
                    )?
                };
                let info = backend::install(&version, false)?;
                self.backend = Some(backend::check_backend());
                Self::refresh_all();
                let scripts = backend::installed_scripts()
                    .iter()
                    .map(|(p, path)| format!("  {p} → {path}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                let text = i18n::tf(
                    "Apache Kafka {version} installed in {dir}.\n\nThe plugin runs it directly with java -cp {dir}/libs/*, so you only need the process:java capability — no script re-approval. If process:java was not granted, enable it in Settings › Plugins › Kafka › Capabilities. Scripts (optional fallback):\n{scripts}\n\nJava 17+ (Kafka 4.x) on the PATH is required.",
                    &[
                        ("version", &version),
                        ("dir", &info.dir),
                        ("scripts", &scripts),
                    ],
                );
                host::notify(
                    NotificationLevel::Info,
                    "Kafka",
                    &i18n::tf("CLI {version} installed.", &[("version", &version)]),
                );
                Ok(Some(text))
            }
            "check-backend" => {
                self.backend = Some(backend::check_backend());
                host::refresh_view(INSTALL);
                Ok(Some(backend_status_text(self.backend.as_ref().unwrap())))
            }
            "topics" => {
                let _ = self.reload();
                host::reveal_view(TOPICS)?;
                Self::refresh_all();
                Ok(Some(self.status.clone()))
            }
            "topic-create" => {
                let c = self.active()?;
                let name = ask(&i18n::t("New topic name"), None)?;
                let partitions: u32 = ask(&i18n::t("Number of partitions"), Some("3"))?
                    .parse()
                    .map_err(|_| i18n::t("Invalid number of partitions."))?;
                let replication: u32 = ask(&i18n::t("Replication factor"), Some("1"))?
                    .parse()
                    .map_err(|_| i18n::t("Invalid replication factor."))?;
                let raw = ask(
                    &i18n::t("Optional configurations (key=value, comma separated)"),
                    Some(""),
                )?;
                let configs = parse_configs(&raw)?;
                backend::create_topic(&c, &name, partitions, replication, &configs)?;
                let _ = self.reload();
                Self::refresh_all();
                let text = i18n::tf("Topic {name} created.", &[("name", &name)]);
                host::notify(NotificationLevel::Info, "Kafka", &text);
                Ok(Some(text))
            }
            "topic-delete" => {
                let c = self.active()?;
                let name = self.choose_topic(arg)?;
                let body = i18n::tf(
                    "PERMANENTLY delete topic {name} and all its messages from cluster {cluster}?",
                    &[("name", &name), ("cluster", &c.name)],
                );
                if !host::confirm(&i18n::t("Kafka: delete topic"), &body)? {
                    return Err(Error::Cancelled);
                }
                backend::delete_topic(&c, &name)?;
                let _ = self.reload();
                Self::refresh_all();
                let text = i18n::tf("Topic {name} deleted.", &[("name", &name)]);
                host::notify(NotificationLevel::Warning, "Kafka", &text);
                Ok(Some(text))
            }
            "topic-details" => {
                let c = self.active()?;
                let name = self.choose_topic(arg)?;
                let topic = self.inspect_topic(&c, &name)?;
                self.selected = Selection::Topic(name.clone());
                host::reveal_view(EXPLORER)?;
                Self::refresh_all();
                Ok(Some(topic_text(&topic)))
            }
            "partitions" => {
                let c = self.active()?;
                let name = self.choose_topic(arg)?;
                let topic = self.inspect_topic(&c, &name)?;
                self.selected = Selection::Topic(name);
                host::reveal_view(EXPLORER)?;
                Self::refresh_all();
                Ok(Some(partitions_text(&topic)))
            }
            "consume" => {
                let c = self.active()?;
                let topic = self.choose_topic(arg)?;
                let limit = config::max_messages();
                let opts = self.consume_options(&topic, limit);
                let count = self.consume(&c, &opts)?;
                self.note(i18n::tf(
                    "{count} messages read from {topic}",
                    &[("count", &count.to_string()), ("topic", &topic)],
                ));
                host::reveal_view(MESSAGES)?;
                self.post_state();
                Self::refresh_all();
                Ok(Some(i18n::tf(
                    "{count} messages read from {topic}.",
                    &[("count", &count.to_string()), ("topic", &topic)],
                )))
            }
            "consume-live" => {
                let c = self.active()?;
                let topic = self.choose_topic(arg)?;
                let opts = self.consume_options(&topic, config::max_messages());
                let mut opts = opts;
                opts.live = true;
                let log = backend::live_log(&c, &opts)?;
                let key = format!("kafka-live-{}", sanitize(&topic));
                let title = i18n::tf("Kafka: {topic} (live)", &[("topic", &topic)]);
                host::open_log_tab(&key, &title, &log)?;
                Ok(Some(i18n::tf(
                    "Following {topic} in a live log tab.",
                    &[("topic", &topic)],
                )))
            }
            "publish" => {
                let c = self.active()?;
                let topic = self.choose_topic(arg)?;
                let key = ask(&i18n::t("Key (empty = no key)"), Some(""))?;
                let value = ask_multiline(&i18n::t("Message value"), Some(""))?;
                let headers_raw = ask(&i18n::t("Headers (key=value, comma separated)"), Some(""))?;
                let headers = parse_configs(&headers_raw)?;
                let partition_raw =
                    ask(&i18n::t("Target partition (empty = automatic)"), Some(""))?;
                let partition = if partition_raw.trim().is_empty() {
                    None
                } else {
                    Some(
                        partition_raw
                            .trim()
                            .parse::<i32>()
                            .map_err(|_| i18n::t("Invalid partition."))?,
                    )
                };
                let msg = PublishMessage {
                    topic: topic.clone(),
                    key: (!key.is_empty()).then_some(key),
                    value,
                    headers,
                    partition,
                };
                backend::publish(&c, &msg)?;
                let text = i18n::tf("Message published to {topic}.", &[("topic", &topic)]);
                host::notify(NotificationLevel::Info, "Kafka", &text);
                Ok(Some(text))
            }
            "messages" => {
                host::reveal_view(MESSAGES)?;
                Ok(None)
            }
            "clear-messages" => {
                self.messages.clear();
                self.post_state();
                host::refresh_view(MESSAGES);
                Ok(None)
            }
            "brokers" => {
                let _ = self.reload();
                host::reveal_view(BROKERS)?;
                Self::refresh_all();
                Ok(Some(cluster_text(&self.cluster, &self.notes)))
            }
            "groups" => {
                let c = self.active()?;
                self.groups = backend::list_groups(&c).unwrap_or_else(|e| {
                    self.notes.push(e.to_string());
                    Vec::new()
                });
                host::reveal_view(GROUPS)?;
                Self::refresh_all();
                Ok(Some(i18n::tf(
                    "{count} consumer groups.",
                    &[("count", &self.groups.len().to_string())],
                )))
            }
            "group-details" | "group-lag" => {
                let c = self.active()?;
                let group = match arg.filter(|s| !s.is_empty()) {
                    Some(g) => g.to_owned(),
                    None => pick_one(
                        &i18n::t("Kafka: consumer group"),
                        &self.groups,
                        &i18n::t("No consumer group found."),
                    )?,
                };
                let rows = backend::describe_group(&c, &group)?;
                self.group_lags = rows.iter().filter(|r| r.group == group).cloned().collect();
                self.selected = Selection::Group(group.clone());
                host::reveal_view(GROUPS)?;
                Self::refresh_all();
                Ok(Some(lag_text(&self.group_lags)))
            }
            "refresh-catalog" => {
                self.versions = backend::available_versions();
                host::refresh_view(INSTALL);
                Ok(Some(i18n::tf(
                    "{count} versions available.",
                    &[("count", &self.versions.len().to_string())],
                )))
            }
            "copy-paths" => {
                let paths = backend::installed_scripts()
                    .iter()
                    .map(|(p, path)| format!("{p} → {path}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                let _ = host::copy_to_clipboard(&paths);
                Ok(Some(paths))
            }
            _ => Err(i18n::tf("Unknown command: {id}", &[("id", id)]).into()),
        }
    }

    fn handle_message_action(&mut self, action: &str, payload: &Value) -> Result<(), Error> {
        match action {
            "state" => {}
            "prefs" => {
                self.remember_formats(payload);
                self.prefs.patch(payload);
                self.prefs.save();
            }
            "consume" => {
                self.remember_formats(payload);
                let c = self.active()?;
                let topic = payload["topic"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .unwrap_or(&self.prefs.topic)
                    .to_owned();
                self.prefs.patch(payload);
                if topic.is_empty() {
                    self.messages.last_error = Some(i18n::t("Choose a topic."));
                } else {
                    let opts = self.consume_options(&topic, self.prefs.limit);
                    match self.consume(&c, &opts) {
                        Ok(count) => {
                            self.messages.last_error = None;
                            self.note(i18n::tf(
                                "{count} messages read from {topic}",
                                &[("count", &count.to_string()), ("topic", &topic)],
                            ));
                        }
                        Err(e) => {
                            self.messages.last_error = Some(e.to_string());
                            self.note(i18n::tf(
                                "Failed to consume: {error}",
                                &[("error", &e.to_string())],
                            ));
                        }
                    }
                    self.prefs.save();
                }
            }
            "live" => {
                let c = self.active()?;
                let topic = self.prefs.topic.clone();
                if topic.is_empty() {
                    self.messages.last_error = Some(i18n::t("Choose a topic."));
                } else {
                    let mut opts = self.consume_options(&topic, config::max_messages());
                    opts.live = true;
                    let log = backend::live_log(&c, &opts)?;
                    host::open_log_tab(
                        &format!("kafka-live-{}", sanitize(&topic)),
                        &i18n::tf("Kafka: {topic} (live)", &[("topic", &topic)]),
                        &log,
                    )?;
                }
            }
            "clear" => {
                self.messages.clear();
                self.note(i18n::t("Messages cleared."));
            }
            "publish" => {
                self.remember_formats(payload);
                let c = self.active()?;
                let topic = payload["topic"].as_str().unwrap_or("").to_owned();
                let key = payload["key"].as_str().unwrap_or("").to_owned();
                let value = payload["value"].as_str().unwrap_or("").to_owned();
                let headers = parse_configs(payload["headers"].as_str().unwrap_or(""))?;
                let partition = payload["partition"]
                    .as_str()
                    .filter(|s| !s.trim().is_empty())
                    .and_then(|s| s.trim().parse::<i32>().ok());
                let msg = PublishMessage {
                    topic,
                    key: (!key.is_empty()).then_some(key),
                    value,
                    headers,
                    partition,
                };
                match backend::publish(&c, &msg) {
                    Ok(()) => {
                        self.messages.last_error = None;
                        self.note(i18n::tf(
                            "Message published to {topic}",
                            &[("topic", &msg.topic)],
                        ));
                    }
                    Err(e) => {
                        self.messages.last_error = Some(e.to_string());
                        self.note(i18n::tf(
                            "Failed to publish: {error}",
                            &[("error", &e.to_string())],
                        ));
                    }
                }
            }
            _ => {}
        }
        self.post_state();
        Ok(())
    }
}

// ---------------------------------------------------------------- Plugin

impl Plugin for Kafka {
    fn activate(&mut self, _ctx: &openidea_plugin::Context) -> Result<(), Error> {
        let _ = host::subscribe(EventKind::Scheduled);
        self.prefs = ConsumePrefs::load();
        self.versions = backend::BUILTIN_VERSIONS
            .iter()
            .map(|v| (*v).into())
            .collect();
        // Metadados são um cache: falhas (CLI ainda não instalada) não impedem
        // a ativação — a view explica como instalar.
        let _ = self.reload();
        let ready: Result<Option<String>, Error> = Ok(None);
        self.report_result(&ready);
        Ok(())
    }

    fn run_command(
        &mut self,
        id: &str,
        ctx: &Invocation,
        arg: Option<&str>,
    ) -> Result<Option<String>, Error> {
        self.run(id, ctx, arg)
    }

    fn handle_events(&mut self, events: &[Event], _ctx: &Invocation) {
        for event in events {
            if matches!(event, Event::Scheduled) && config::setting_bool("autoRefresh", false) {
                let _ = self.reload();
                Self::refresh_all();
            }
        }
    }
}

// ---------------------------------------------------------------- Views

impl Views for Kafka {
    fn render(
        &mut self,
        view: &str,
        viewport: Option<Viewport>,
        _ctx: &Invocation,
    ) -> Result<View, Error> {
        match view {
            EXPLORER => Ok(self.render_explorer()),
            TOPICS => Ok(self.render_topics(viewport)),
            MESSAGES => Ok(self.render_messages()),
            CONNECTIONS => Ok(self.render_connections()),
            BROKERS => Ok(self.render_brokers()),
            GROUPS => Ok(self.render_groups(viewport)),
            INSTALL => Ok(self.render_install()),
            _ => Err(Error::NotFound(view.into())),
        }
    }

    fn handle(&mut self, view: &str, event: ViewEvent, ctx: &Invocation) -> Result<(), Error> {
        match view {
            EXPLORER => self.handle_explorer(event, ctx),
            TOPICS => self.handle_topics(event, ctx),
            MESSAGES => self.handle_messages(event, ctx),
            CONNECTIONS => self.handle_connections(event, ctx),
            BROKERS => self.handle_simple(&event, ctx, BROKERS),
            GROUPS => self.handle_groups(event, ctx),
            INSTALL => self.handle_simple(&event, ctx, INSTALL),
            _ => Err(Error::NotFound(view.into())),
        }
    }
}

impl Kafka {
    fn render_explorer(&self) -> View {
        let profiles = config::load_profiles();
        let active = config::active_id();
        if profiles.is_empty() {
            return View::message(
                "No connections yet. Use 'Kafka: New Connection' (or the + button) and, if needed, 'Kafka: Download and Install the Apache Kafka CLI'.",
            )
            .button(Button::new("connection-new", "New Connection").icon("plus"))
            .button(Button::new("install-backend", "Install CLI").icon("download"))
            .status(&self.status);
        }
        let mut items = Vec::new();
        for p in &profiles {
            let key = format!("conn:{}", p.id);
            let is_active = active.as_deref() == Some(p.id.as_str());
            let expanded = self.expanded.contains(&key) || is_active;
            items.push(
                TreeItem::new(
                    &key,
                    &format!("{} {}", if is_active { "●" } else { "○" }, p.name),
                )
                .icon("server")
                .detail(&p.bootstrap_servers)
                .expandable(expanded),
            );
            if !expanded {
                continue;
            }
            let topics_key = format!("cat:{}:topics", p.id);
            let brokers_key = format!("cat:{}:brokers", p.id);
            let groups_key = format!("cat:{}:groups", p.id);
            let categories: [(&String, &str, &str, usize); 3] = [
                (
                    &topics_key,
                    "Topics ({count})",
                    "folder",
                    self.topic_names.len(),
                ),
                (
                    &brokers_key,
                    "Brokers ({count})",
                    "server",
                    self.cluster.brokers.len(),
                ),
                (
                    &groups_key,
                    "Consumer groups ({count})",
                    "module",
                    self.groups.len(),
                ),
            ];
            for (key, label, icon, count) in categories {
                let label = i18n::tf(label, &[("count", &count.to_string())]);
                items.push(
                    TreeItem::new(key, &label)
                        .parent(&format!("conn:{}", p.id))
                        .icon(icon)
                        .expandable(self.expanded.contains(key)),
                );
            }
            if self.expanded.contains(&topics_key) {
                for name in &self.topic_names {
                    let key = format!("topic:{}:{}", p.id, name);
                    let detail = self
                        .topics
                        .iter()
                        .find(|t| &t.name == name)
                        .map(|t| {
                            i18n::tf(
                                "{count} partitions · RF {factor}",
                                &[
                                    ("count", &t.partitions.to_string()),
                                    ("factor", &t.replication_factor.to_string()),
                                ],
                            )
                        })
                        .unwrap_or_default();
                    items.push(
                        TreeItem::new(&key, name)
                            .parent(&topics_key)
                            .icon("file")
                            .detail(&detail)
                            .expandable(self.expanded.contains(&key)),
                    );
                    if self.expanded.contains(&key)
                        && let Some(topic) = self.topics.iter().find(|t| &t.name == name)
                    {
                        for part in &topic.details {
                            items.push(
                                TreeItem::new(
                                    &format!("part:{}:{}:{}", p.id, name, part.id),
                                    &i18n::tf("Partition {id}", &[("id", &part.id.to_string())]),
                                )
                                .parent(&key)
                                .icon("split")
                                .detail(&i18n::tf(
                                    "leader {leader} · ISR {isr}",
                                    &[
                                        ("leader", &part.leader.unwrap_or(-1).to_string()),
                                        ("isr", &format_ids(&part.isr)),
                                    ],
                                )),
                            );
                        }
                    }
                }
            }
            if self.expanded.contains(&brokers_key) {
                for b in &self.cluster.brokers {
                    items.push(
                        TreeItem::new(
                            &format!("broker:{}:{}", p.id, b.id),
                            &i18n::tf("Broker {id}", &[("id", &b.id.to_string())]),
                        )
                        .parent(&brokers_key)
                        .icon("server")
                        .detail(&i18n::tf(
                            "{host}:{port} · {status}",
                            &[
                                ("host", &b.host),
                                ("port", &b.port.to_string()),
                                ("status", &b.status),
                            ],
                        )),
                    );
                }
            }
            if self.expanded.contains(&groups_key) {
                for g in &self.groups {
                    items.push(
                        TreeItem::new(&format!("group:{}:{}", p.id, g), g)
                            .parent(&groups_key)
                            .icon("module"),
                    );
                }
            }
        }
        View::new(Body::Tree(items))
            .button(Button::new("connection-new", "New").icon("plus"))
            .button(Button::new("refresh", "Refresh").icon("refresh"))
            .button(Button::new("install-backend", "Install CLI").icon("download"))
            .button(Button::new("consume-live", "Live").icon("run"))
            .status(&self.status)
    }

    fn handle_explorer(&mut self, event: ViewEvent, ctx: &Invocation) -> Result<(), Error> {
        let activating = matches!(&event, ViewEvent::Activate(_));
        match event {
            ViewEvent::Button(id) => {
                let _ = self.run(&id, ctx, None);
                host::refresh_view(EXPLORER);
            }
            ViewEvent::Expand(key) => {
                self.expanded.insert(key);
                host::refresh_view(EXPLORER);
            }
            ViewEvent::Collapse(key) => {
                self.expanded.remove(&key);
                host::refresh_view(EXPLORER);
            }
            ViewEvent::Select(key) | ViewEvent::Activate(key) => {
                self.selected = selection_from_key(&key);
                if let Selection::Topic(name) = self.selected.clone()
                    && let Ok(c) = self.active()
                {
                    let _ = self.inspect_topic(&c, &name);
                }
                if activating && let Selection::Topic(name) = self.selected.clone() {
                    self.prefs.topic = name;
                    self.prefs.save();
                    let _ = host::reveal_view(MESSAGES);
                    self.post_state();
                }
                host::refresh_view(EXPLORER);
            }
            _ => {}
        }
        Ok(())
    }

    fn render_topics(&mut self, viewport: Option<Viewport>) -> View {
        let mut rows: Vec<Vec<String>> = self
            .topics
            .iter()
            .map(|t| {
                let chosen = config::formats_for(&t.name);
                vec![
                    t.name.clone(),
                    t.partitions.to_string(),
                    t.replication_factor.to_string(),
                    format_configs(&t.configs),
                    t.details
                        .iter()
                        .map(|p| {
                            p.leader
                                .map(|l| l.to_string())
                                .unwrap_or_else(|| "-".into())
                        })
                        .collect::<Vec<_>>()
                        .join(", "),
                    chosen.serializer,
                    chosen.deserializer,
                ]
            })
            .collect();
        if let Some(sort) = self.sort
            && sort.column < 7
        {
            rows.sort_by(|a, b| {
                let ord = a[sort.column as usize].cmp(&b[sort.column as usize]);
                if sort.descending { ord.reverse() } else { ord }
            });
        }
        let total = rows.len() as u32;
        let range = Viewport::range(viewport, total, 200);
        let shown = rows[range.start as usize..range.end as usize].to_vec();
        let table = Table {
            columns: [
                "Topic",
                "Partitions",
                "RF",
                "Configurations",
                "Leaders",
                "Serializer",
                "Deserializer",
            ]
            .iter()
            .map(|s| (*s).into())
            .collect(),
            total_rows: total,
            first_row: range.start,
            rows: shown,
            editable: false,
        };
        View::new(Body::sortable_table(&table, self.sort))
            .button(Button::new("refresh", "Refresh").icon("refresh"))
            .button(Button::new("topic-create", "Create Topic").icon("plus"))
            .button(Button::new("topic-delete", "Delete").icon("trash"))
            .button(Button::new("consume", "Consume").icon("run"))
            .button(Button::new("publish", "Publish").icon("export"))
            .button(Button::new("topic-details", "Details").icon("info"))
            .status(&self.status)
    }

    fn handle_topics(&mut self, event: ViewEvent, ctx: &Invocation) -> Result<(), Error> {
        match event {
            ViewEvent::Button(id) => {
                let name = match &self.selected {
                    Selection::Topic(n) => Some(n.clone()),
                    _ => None,
                };
                let arg = [
                    "topic-delete",
                    "topic-details",
                    "consume",
                    "publish",
                    "partitions",
                ]
                .contains(&id.as_str())
                .then_some(name)
                .flatten();
                let result = self.run(&id, ctx, arg.as_deref());
                if let Err(e) = result {
                    host::notify(NotificationLevel::Error, "Kafka", &e.to_string());
                }
                host::refresh_view(TOPICS);
            }
            other => {
                if let Some(sort) = other.sort() {
                    self.sort = Some(sort);
                } else if let Some(row) = other.table_select().or_else(|| other.table_activate())
                    && let Some(t) = self.topics.get(row as usize)
                {
                    self.selected = Selection::Topic(t.name.clone());
                }
                host::refresh_view(TOPICS);
            }
        }
        Ok(())
    }

    fn render_messages(&self) -> View {
        let start = self.messages_state().replace('<', "\\u003c");
        let page = PAGE.replacen(r#""__START__""#, &start, 1);
        View::new(Body::Html(page))
            .button(Button::new("refresh", "Refresh").icon("refresh"))
            .button(Button::new("clear-messages", "Clear").icon("trash"))
            .button(Button::new("consume-live", "Live").icon("run"))
            .status(&self.status)
    }

    fn handle_messages(&mut self, event: ViewEvent, ctx: &Invocation) -> Result<(), Error> {
        match event {
            ViewEvent::Message(text) => {
                let Ok(value) = serde_json::from_str::<Value>(&text) else {
                    return Ok(());
                };
                let action = value["action"].as_str().unwrap_or("state");
                self.handle_message_action(action, &value)?;
            }
            ViewEvent::Button(id) => {
                if id == "clear-messages" {
                    self.messages.clear();
                    self.post_state();
                } else {
                    let arg = (id == "consume-live" && !self.prefs.topic.is_empty())
                        .then(|| self.prefs.topic.clone());
                    let result = self.run(&id, ctx, arg.as_deref());
                    if let Err(e) = result {
                        host::notify(NotificationLevel::Error, "Kafka", &e.to_string());
                    }
                    self.post_state();
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn render_connections(&self) -> View {
        let draft = self.draft.clone().unwrap_or_else(|| {
            config::active()
                .or_else(|| config::load_profiles().into_iter().next())
                .unwrap_or_else(|| Connection::new("", "New cluster"))
        });
        let choice = |options: &[&str], current: &str| {
            FieldValue::Choice(
                options.iter().map(|s| (*s).into()).collect(),
                current.into(),
            )
        };
        let fields = vec![
            Field::new(
                "info",
                "Connections",
                FieldValue::label(&i18n::tf(
                    "{count} profile(s) · active: {active}. The plugin never stores passwords.",
                    &[
                        ("count", &config::load_profiles().len().to_string()),
                        (
                            "active",
                            &config::active_id().unwrap_or_else(|| i18n::t("none")),
                        ),
                    ],
                )),
            ),
            Field::new(
                "name",
                "Connection name",
                FieldValue::Text(draft.name.clone()),
            ),
            Field::new(
                "bootstrap_servers",
                "Bootstrap servers (host:port, …)",
                FieldValue::Text(draft.bootstrap_servers.clone()),
            ),
            Field::new(
                "security_protocol",
                "Security protocol",
                choice(&config::SECURITY_PROTOCOLS, &draft.security_protocol),
            ),
            Field::new(
                "sasl_mechanism",
                "SASL mechanism",
                choice(&config::SASL_MECHANISMS, &draft.sasl_mechanism),
            ),
            Field::new(
                "sasl_username",
                "SASL username (optional)",
                FieldValue::Text(draft.sasl_username.clone()),
            ),
            Field::new(
                "client_properties",
                "Client properties file (credentials)",
                FieldValue::Text(draft.client_properties.clone()),
            ),
            Field::new(
                "sasl_jaas_config",
                "SASL JAAS config (inline, dev only)",
                FieldValue::Multiline(draft.sasl_jaas_config.clone()),
            ),
            Field::new(
                "kcat_config",
                "kcat config file (optional)",
                FieldValue::Text(draft.kcat_config.clone()),
            ),
            Field::new(
                "tls_ca",
                "TLS: CA / truststore",
                FieldValue::Text(draft.tls_ca.clone()),
            ),
            Field::new(
                "tls_cert",
                "TLS: certificate",
                FieldValue::Text(draft.tls_cert.clone()),
            ),
            Field::new(
                "tls_key",
                "TLS: key",
                FieldValue::Text(draft.tls_key.clone()),
            ),
            Field::new(
                "tls_truststore",
                "Java: truststore (JKS)",
                FieldValue::Text(draft.tls_truststore.clone()),
            ),
            Field::new(
                "tls_keystore",
                "Java: keystore (JKS)",
                FieldValue::Text(draft.tls_keystore.clone()),
            ),
            Field::new(
                "extra_properties",
                "Extra properties (key=value, one per line)",
                FieldValue::Multiline(draft.extra_properties.join("\n")),
            ),
            Field::new(
                "security",
                "Security",
                FieldValue::label(&i18n::t(
                    "For development, paste sasl.jaas.config in the field above: it is saved in plain text with the connection and written to a private file for the CLI. A client properties file keeps credentials outside the plugin. Sensitive keys in Extra properties are rejected.",
                )),
            ),
        ];
        View::new(Body::Form(fields))
            .button(Button::new("connection-new", "New").icon("plus"))
            .button(Button::new("connection-save", "Test and Save").icon("check"))
            .button(Button::new("connection-test-draft", "Test").icon("run"))
            .button(Button::new("connection-edit", "Edit").icon("settings"))
            .button(Button::new("connection-activate", "Activate").icon("check"))
            .button(Button::new("connection-delete", "Delete").icon("trash"))
            .status(&self.status)
    }

    fn handle_connections(&mut self, event: ViewEvent, ctx: &Invocation) -> Result<(), Error> {
        match event {
            ViewEvent::Field(key, value) => {
                let draft = self.draft.get_or_insert_with(|| {
                    config::active().unwrap_or_else(|| Connection::new("", "New cluster"))
                });
                draft.apply_field(&key, &value);
                host::refresh_view(CONNECTIONS);
            }
            ViewEvent::Button(id) => {
                let result = self.run(&id, ctx, None);
                if let Err(e) = result {
                    host::notify(NotificationLevel::Error, "Kafka", &e.to_string());
                }
                host::refresh_view(CONNECTIONS);
            }
            _ => {}
        }
        Ok(())
    }

    fn render_brokers(&self) -> View {
        let rows: Vec<Vec<String>> = self
            .cluster
            .brokers
            .iter()
            .map(|b| {
                vec![
                    b.id.to_string(),
                    b.host.clone(),
                    b.port.to_string(),
                    b.rack.clone(),
                    b.status.clone(),
                ]
            })
            .collect();
        let table = Table {
            columns: ["ID", "Host", "Port", "Rack", "Status"]
                .iter()
                .map(|s| (*s).into())
                .collect(),
            total_rows: rows.len() as u32,
            first_row: 0,
            rows,
            editable: false,
        };
        View::new(Body::sortable_table(&table, None))
            .button(Button::new("refresh", "Refresh").icon("refresh"))
            .button(Button::new("check-backend", "Verify CLI").icon("settings"))
            .status(&cluster_text(&self.cluster, &self.notes))
    }

    fn render_groups(&self, viewport: Option<Viewport>) -> View {
        let rows: Vec<Vec<String>> = if self.group_lags.is_empty() {
            self.groups
                .iter()
                .map(|g| {
                    vec![
                        g.clone(),
                        "-".into(),
                        "-".into(),
                        "-".into(),
                        "-".into(),
                        "-".into(),
                        "-".into(),
                        "-".into(),
                    ]
                })
                .collect()
        } else {
            self.group_lags
                .iter()
                .map(|r| {
                    vec![
                        r.group.clone(),
                        r.topic.clone(),
                        r.partition.to_string(),
                        r.current
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "-".into()),
                        r.log_end.to_string(),
                        r.lag.to_string(),
                        r.consumer_id.clone(),
                        r.client_id.clone(),
                    ]
                })
                .collect()
        };
        let total = rows.len() as u32;
        let range = Viewport::range(viewport, total, 200);
        let table = Table {
            columns: [
                "Group",
                "Topic",
                "Partition",
                "Current offset",
                "Log-end offset",
                "Lag",
                "Consumer",
                "Client",
            ]
            .iter()
            .map(|s| (*s).into())
            .collect(),
            total_rows: total,
            first_row: range.start,
            rows: rows[range.start as usize..range.end as usize].to_vec(),
            editable: false,
        };
        View::new(Body::sortable_table(&table, None))
            .button(Button::new("refresh", "Refresh").icon("refresh"))
            .button(Button::new("group-details", "Group Lag").icon("info"))
            .status(&self.status)
    }

    fn handle_groups(&mut self, event: ViewEvent, ctx: &Invocation) -> Result<(), Error> {
        match event {
            ViewEvent::Button(id) => {
                let group = match &self.selected {
                    Selection::Group(g) => Some(g.clone()),
                    _ => None,
                };
                let result = self.run(&id, ctx, group.as_deref());
                if let Err(e) = result {
                    host::notify(NotificationLevel::Error, "Kafka", &e.to_string());
                }
                host::refresh_view(GROUPS);
            }
            ViewEvent::Select(key) | ViewEvent::Activate(key) => {
                if let Some(group) = key.strip_prefix("group:") {
                    self.selected =
                        Selection::Group(group.split(':').last().unwrap_or(group).into());
                }
            }
            other => {
                if let Some(row) = other.table_select().or_else(|| other.table_activate()) {
                    let name = if self.group_lags.is_empty() {
                        self.groups.get(row as usize).cloned()
                    } else {
                        self.group_lags.get(row as usize).map(|r| r.group.clone())
                    };
                    if let Some(name) = name {
                        self.selected = Selection::Group(name);
                        host::refresh_view(GROUPS);
                    }
                }
            }
        }
        Ok(())
    }

    fn render_install(&self) -> View {
        let mut rows: Vec<Vec<String>> = Vec::new();
        if let Some(info) = config::install_info() {
            rows.push(vec![
                i18n::t("Distribution"),
                i18n::tf("Apache Kafka {version}", &[("version", &info.version)]),
                info.dir.clone(),
            ]);
        } else {
            rows.push(vec![
                i18n::t("Distribution"),
                i18n::t("not installed"),
                i18n::t("Use 'Download and install'"),
            ]);
        }
        if let Some(status) = &self.backend {
            for (program, state) in &status.programs {
                rows.push(vec![program.clone(), state.clone(), "-".into()]);
            }
            if let Some(java) = &status.java {
                rows.push(vec![
                    i18n::t("Java"),
                    java.clone(),
                    i18n::t("needed for the scripts"),
                ]);
            }
        } else {
            rows.push(vec![
                i18n::t("Programs"),
                i18n::t("click 'Verify CLI'"),
                "-".into(),
            ]);
        }
        for version in &self.versions {
            rows.push(vec![
                i18n::t("Available version"),
                version.clone(),
                i18n::t("select to install"),
            ]);
        }
        let table = Table {
            columns: ["Item", "State", "Detail"]
                .iter()
                .map(|s| (*s).into())
                .collect(),
            total_rows: rows.len() as u32,
            first_row: 0,
            rows,
            editable: false,
        };
        let base = match config::install_info() {
            Some(info) => i18n::tf(
                "Installed in {dir}. The CLI runs directly with process:java and {bin}/../libs. Script capabilities are only needed for a Kafka installed outside the plugin.",
                &[("dir", &info.dir), ("bin", &info.bin)],
            ),
            None => i18n::t(
                "Download the official distribution and point the process:* capabilities to the extracted scripts.",
            ),
        };
        let hint = i18n::tf(
            "{base} Configured version: {version}. Requires Java on the PATH.",
            &[("base", &base), ("version", &config::kafka_version())],
        );
        View::new(Body::sortable_table(&table, None))
            .button(Button::new("install-backend", "Download and Install").icon("download"))
            .button(Button::new("refresh-catalog", "Refresh Catalog").icon("refresh"))
            .button(Button::new("check-backend", "Verify CLI").icon("settings"))
            .button(Button::new("copy-paths", "Copy Paths").icon("copy"))
            .status(&hint)
    }

    fn handle_simple(
        &mut self,
        event: &ViewEvent,
        ctx: &Invocation,
        view: &str,
    ) -> Result<(), Error> {
        if let ViewEvent::Button(id) = event {
            let result = self.run(id, ctx, None);
            if let Err(e) = result {
                host::notify(NotificationLevel::Error, "Kafka", &e.to_string());
            }
            host::refresh_view(view);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------- Details

impl Details for Kafka {
    fn detail(&mut self, view: &str, key: &str, _ctx: &Invocation) -> Result<Vec<Tab>, Error> {
        if view != EXPLORER {
            return Err(Error::NotFound(view.into()));
        }
        let c = config::active();
        let mut tabs = Vec::new();
        match selection_from_key(key) {
            Selection::Topic(name) => {
                let topic = self
                    .selected_topic
                    .clone()
                    .filter(|t| t.name == name)
                    .or_else(|| self.topics.iter().find(|t| t.name == name).cloned())
                    .unwrap_or_else(|| Topic {
                        name: name.clone(),
                        ..Topic::default()
                    });
                tabs.push(Tab::new(
                    "details",
                    &i18n::t("Details"),
                    Pane::Text(topic_text(&topic)),
                ));
                tabs.push(Tab::new(
                    "partitions",
                    &i18n::t("Partitions"),
                    Pane::Text(partitions_text(&topic)),
                ));
                tabs.push(Tab::new(
                    "configs",
                    &i18n::t("Configurations"),
                    Pane::Text(configs_text(&topic)),
                ));
                if let Some(c) = c {
                    let mut opts = self.consume_options(&name, config::max_messages());
                    opts.live = true;
                    tabs.push(Tab::new(
                        "live",
                        &i18n::t("Live messages"),
                        Pane::Log(backend::live_log(&c, &opts)?),
                    ));
                }
            }
            Selection::Partition(name, id) => {
                let text = self
                    .selected_topic
                    .as_ref()
                    .filter(|t| t.name == name)
                    .and_then(|t| t.details.iter().find(|p| p.id == id))
                    .map(partition_text)
                    .unwrap_or_else(|| {
                        i18n::tf(
                            "Partition {id} of {name}: refresh the metadata.",
                            &[("id", &id.to_string()), ("name", name.as_str())],
                        )
                    });
                tabs.push(Tab::new("details", &i18n::t("Details"), Pane::Text(text)));
            }
            Selection::Broker(id) => {
                let text = self
                    .cluster
                    .brokers
                    .iter()
                    .find(|b| b.id == id)
                    .map(broker_text)
                    .unwrap_or_else(|| {
                        i18n::tf(
                            "Broker {id} not found in the metadata.",
                            &[("id", &id.to_string())],
                        )
                    });
                tabs.push(Tab::new("details", &i18n::t("Details"), Pane::Text(text)));
            }
            Selection::Group(name) => {
                let rows: Vec<GroupLag> = self
                    .group_lags
                    .iter()
                    .filter(|r| r.group == name)
                    .cloned()
                    .collect();
                let text = if rows.is_empty() {
                    i18n::tf(
                        "Group {name}: click 'Group Lag' to query offsets.",
                        &[("name", &name)],
                    )
                } else {
                    lag_text(&rows)
                };
                tabs.push(Tab::new("lag", &i18n::t("Lag"), Pane::Text(text)));
            }
            Selection::Connection(id) => {
                let text = config::by_id(&id)
                    .map(|p| connection_text(&p))
                    .unwrap_or_else(|| i18n::tf("Connection {id} not found.", &[("id", &id)]));
                tabs.push(Tab::new(
                    "details",
                    &i18n::t("Connection"),
                    Pane::Text(text),
                ));
                let scripts = backend::installed_scripts();
                let paths = if scripts.is_empty() {
                    i18n::t("No distribution installed by the plugin yet.")
                } else {
                    scripts
                        .iter()
                        .map(|(p, path)| format!("{p} → {path}"))
                        .collect::<Vec<_>>()
                        .join("\n")
                };
                tabs.push(Tab::new(
                    "scripts",
                    &i18n::t("CLI scripts"),
                    Pane::Text(paths),
                ));
            }
            Selection::None => {
                tabs.push(Tab::new(
                    "empty",
                    &i18n::t("Details"),
                    Pane::Message(i18n::t(
                        "Select a connection, topic, partition, broker or group.",
                    )),
                ));
            }
        }
        Ok(tabs)
    }
}

register_plugin!(Kafka, views, details);

// ---------------------------------------------------------------- helpers

fn ask(prompt: &str, value: Option<&str>) -> Result<String, Error> {
    host::input(prompt, value)?.ok_or(Error::Cancelled)
}

fn ask_multiline(prompt: &str, value: Option<&str>) -> Result<String, Error> {
    host::input_multiline(prompt, value)?.ok_or(Error::Cancelled)
}

fn pick_one(title: &str, items: &[String], empty: &str) -> Result<String, Error> {
    if items.is_empty() {
        return Err(empty.into());
    }
    let picks: Vec<PickItem> = items.iter().map(|i| PickItem::new(i, i)).collect();
    host::pick(title, &picks)?.ok_or(Error::Cancelled)
}

fn parse_configs(raw: &str) -> Result<Vec<(String, String)>, Error> {
    let mut out = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (key, value) = part.split_once('=').ok_or_else(|| {
            i18n::tf(
                "Invalid configuration {part}: use key=value.",
                &[("part", part)],
            )
        })?;
        if config::is_secret_key(key) {
            return Err(i18n::tf(
                "Sensitive configuration {key} is not accepted here.",
                &[("key", key)],
            )
            .into());
        }
        out.push((key.trim().into(), value.trim().into()));
    }
    Ok(out)
}

fn selection_from_key(key: &str) -> Selection {
    if let Some(rest) = key.strip_prefix("topic:") {
        let mut parts = rest.splitn(2, ':');
        let _id = parts.next().unwrap_or("");
        return Selection::Topic(parts.next().unwrap_or("").into());
    }
    if let Some(rest) = key.strip_prefix("part:") {
        let parts: Vec<&str> = rest.split(':').collect();
        if parts.len() >= 3 {
            return Selection::Partition(parts[1].into(), parts[2].parse().unwrap_or(0));
        }
    }
    if let Some(rest) = key.strip_prefix("broker:") {
        return Selection::Broker(rest.rsplit(':').next().unwrap_or("0").parse().unwrap_or(0));
    }
    if let Some(rest) = key.strip_prefix("group:") {
        return Selection::Group(rest.split(':').last().unwrap_or(rest).into());
    }
    if let Some(id) = key.strip_prefix("conn:") {
        return Selection::Connection(id.into());
    }
    Selection::None
}

fn sanitize(text: &str) -> String {
    text.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

fn format_ids(ids: &[i32]) -> String {
    if ids.is_empty() {
        return "-".into();
    }
    ids.iter().map(i32::to_string).collect::<Vec<_>>().join(",")
}

fn format_configs(configs: &[(String, String)]) -> String {
    if configs.is_empty() {
        return "-".into();
    }
    let text = configs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(", ");
    backend::clip(&text, 160)
}

fn topic_text(t: &Topic) -> String {
    let mut out = vec![
        i18n::tf("Topic: {name}", &[("name", &t.name)]),
        i18n::tf("TopicId: {id}", &[("id", t.id.as_deref().unwrap_or("-"))]),
        i18n::tf(
            "Partitions: {count}",
            &[("count", &t.partitions.to_string())],
        ),
        i18n::tf(
            "Replication factor: {factor}",
            &[("factor", &t.replication_factor.to_string())],
        ),
        i18n::tf(
            "Configurations: {configs}",
            &[("configs", &format_configs(&t.configs))],
        ),
    ];
    if !t.details.is_empty() {
        out.push(String::new());
        out.push(partitions_text(t));
    }
    out.join("\n")
}

fn partitions_text(t: &Topic) -> String {
    let mut out = vec![format!(
        "{:<10} {:<8} {:<20} {:<20} {:<14} {:<14} {}",
        i18n::t("Partition"),
        i18n::t("Leader"),
        i18n::t("Replicas"),
        "ISR",
        i18n::t("Start offset"),
        i18n::t("End offset"),
        i18n::t("Lag")
    )];
    for p in &t.details {
        let lag = match (p.earliest, p.latest) {
            (Some(a), Some(b)) => (b - a).max(0).to_string(),
            _ => "-".into(),
        };
        out.push(format!(
            "{:<10} {:<8} {:<20} {:<20} {:<14} {:<14} {}",
            p.id,
            p.leader
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            format_ids(&p.replicas),
            format_ids(&p.isr),
            p.earliest
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            p.latest
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            lag,
        ));
    }
    if t.details.is_empty() {
        out.push(i18n::t(
            "No partitions in the metadata. Refresh the cluster.",
        ));
    }
    out.join("\n")
}

fn partition_text(p: &Partition) -> String {
    let leader = p
        .leader
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".into());
    let start = p
        .earliest
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".into());
    let end = p
        .latest
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".into());
    let lag = match (p.earliest, p.latest) {
        (Some(a), Some(b)) => (b - a).max(0).to_string(),
        _ => "-".into(),
    };
    i18n::tf(
        "Partition: {id}\nLeader: {leader}\nReplicas: {replicas}\nISR: {isr}\nStart offset: {start}\nEnd offset: {end}\nLag (retained messages): {lag}",
        &[
            ("id", &p.id.to_string()),
            ("leader", &leader),
            ("replicas", &format_ids(&p.replicas)),
            ("isr", &format_ids(&p.isr)),
            ("start", &start),
            ("end", &end),
            ("lag", &lag),
        ],
    )
}

fn configs_text(t: &Topic) -> String {
    if t.configs.is_empty() {
        return i18n::t("No topic-specific configuration (broker defaults apply).");
    }
    t.configs
        .iter()
        .map(|(k, v)| format!("{k} = {v}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn broker_text(b: &model::Broker) -> String {
    i18n::tf(
        "Broker {id}\nAddress: {host}:{port}\nRack: {rack}\nStatus: {status}",
        &[
            ("id", &b.id.to_string()),
            ("host", &b.host),
            ("port", &b.port.to_string()),
            ("rack", if b.rack.is_empty() { "-" } else { &b.rack }),
            ("status", &b.status),
        ],
    )
}

fn cluster_text(info: &ClusterInfo, notes: &[String]) -> String {
    let mut out = vec![i18n::tf(
        "Brokers: {count}",
        &[("count", &info.brokers.len().to_string())],
    )];
    if let Some(id) = info.controller_id {
        out.push(i18n::tf(
            "Controller (KRaft LeaderId): {id}",
            &[("id", &id.to_string())],
        ));
    }
    if let Some(q) = &info.quorum {
        if !q.cluster_id.is_empty() {
            out.push(i18n::tf("ClusterId: {id}", &[("id", &q.cluster_id)]));
        }
        if let Some(epoch) = q.leader_epoch {
            out.push(i18n::tf(
                "LeaderEpoch: {epoch}",
                &[("epoch", &epoch.to_string())],
            ));
        }
        if let Some(hw) = q.high_watermark {
            out.push(i18n::tf(
                "HighWatermark: {watermark}",
                &[("watermark", &hw.to_string())],
            ));
        }
        if !q.current_voters.is_empty() {
            out.push(i18n::tf(
                "Voters: {voters}",
                &[("voters", &q.current_voters)],
            ));
        }
    }
    for note in notes {
        out.push(i18n::tf(
            "Warning: {note}",
            &[("note", &backend::clip(note, 200))],
        ));
    }
    out.join(" · ")
}

fn lag_text(rows: &[GroupLag]) -> String {
    if rows.is_empty() {
        return i18n::t("No offsets/lag available for this group.");
    }
    let mut out = vec![format!(
        "{:<12} {:<8} {:<16} {:<16} {:<10}",
        i18n::t("Topic"),
        i18n::t("Partition"),
        i18n::t("Current offset"),
        i18n::t("Log-end offset"),
        i18n::t("Lag")
    )];
    for r in rows {
        out.push(format!(
            "{:<12} {:<8} {:<16} {:<16} {:<10}",
            r.topic,
            r.partition,
            r.current
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            r.log_end,
            r.lag,
        ));
    }
    let total: i64 = rows.iter().map(|r| r.lag.max(0)).sum();
    out.push(i18n::tf("Total lag: {lag}", &[("lag", &total.to_string())]));
    out.join("\n")
}

fn connection_text(c: &Connection) -> String {
    let file = if c.client_properties.is_empty() {
        i18n::t("none")
    } else {
        c.client_properties.clone()
    };
    let jaas = if c.sasl_jaas_config.trim().is_empty() {
        i18n::t("none")
    } else {
        i18n::t("inline")
    };
    let extra = if c.extra_properties.is_empty() {
        i18n::t("none yet")
    } else {
        c.extra_properties.join(", ")
    };
    i18n::tf(
        "Connection: {name}\nBootstrap servers: {servers}\nSecurity: {security}\nClient properties file: {file}\nSASL JAAS config: {jaas}\nExtra properties: {extra}",
        &[
            ("name", &c.name),
            ("servers", &c.bootstrap_servers),
            ("security", &c.security_summary()),
            ("file", &file),
            ("jaas", &jaas),
            ("extra", &extra),
        ],
    )
}

fn backend_status_text(status: &BackendStatus) -> String {
    let mut out = Vec::new();
    if let Some(info) = &status.installed {
        out.push(i18n::tf(
            "CLI installed: Apache Kafka {version} at {dir}",
            &[("version", &info.version), ("dir", &info.dir)],
        ));
    } else {
        out.push(i18n::t("CLI installed: no"));
    }
    if let Some(java) = &status.java {
        out.push(i18n::tf("Java: {version}", &[("version", java)]));
    }
    for (program, state) in &status.programs {
        out.push(format!("{program}: {state}"));
    }
    out.join("\n")
}

// ---------------------------------------------------------------- testes

/// O MockHost não concede `process:*` por padrão: os testes aprovam todos os
/// programas que o plugin pode executar, como o usuário faz ao instalar.
#[cfg(test)]
pub(crate) fn mock_host() -> openidea_plugin::mock::MockHost {
    use openidea_plugin::mock::MockHost;
    MockHost::install().with_capabilities(&[
        "project.read",
        "storage",
        "events",
        "clipboard",
        "secrets",
        "net:downloads.apache.org",
        "net:archive.apache.org",
        "fs.read:/",
        "fs.write:/",
        "process:curl",
        "process:tar",
        "process:java",
        "process:kcat",
        "process:kafka-topics.sh",
        "process:kafka-console-consumer.sh",
        "process:kafka-console-producer.sh",
        "process:kafka-consumer-groups.sh",
        "process:kafka-get-offsets.sh",
        "process:kafka-run-class.sh",
        "process:kafka-broker-api-versions.sh",
        "process:kafka-metadata-quorum.sh",
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use openidea_plugin::mock::MockHost;
    use openidea_plugin::views::ViewEvent;

    fn ok(stdout: &str) -> Result<openidea_plugin::ProcessOutput, Error> {
        Ok(openidea_plugin::ProcessOutput {
            status: Some(0),
            stdout: stdout.into(),
            stderr: String::new(),
        })
    }

    fn host_with_cluster() -> MockHost {
        let host = mock_host();
        host.on_process(backend::TOPICS, |args, _| {
            if args.contains(&"--list".to_owned()) {
                ok("pedidos\nclientes\n")
            } else if args.contains(&"--describe".to_owned()) {
                ok(concat!(
                    "Topic: pedidos\tTopicId: aaa\tPartitionCount: 2\tReplicationFactor: 2\tConfigs: cleanup.policy=compact\n",
                    "\tTopic: pedidos\tPartition: 0\tLeader: 1\tReplicas: 1,2\tIsr: 1,2\n",
                    "\tTopic: pedidos\tPartition: 1\tLeader: 2\tReplicas: 2,1\tIsr: 2,1\n",
                    "Topic: clientes\tTopicId: bbb\tPartitionCount: 1\tReplicationFactor: 1\tConfigs: \n",
                    "\tTopic: clientes\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\n",
                ))
            } else {
                ok("")
            }
        });
        host.on_process(backend::GROUPS, |args, _| {
            if args.contains(&"--list".to_owned()) {
                ok("g1\n")
            } else {
                ok(concat!(
                    "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\n",
                    "g1 pedidos 0 10 12 2 c1 /h client-1\n",
                ))
            }
        });
        host.on_process(backend::OFFSETS, |args, _| {
            if args.iter().any(|a| a == "-2") {
                ok("pedidos:0:0\npedidos:1:0\n")
            } else {
                ok("pedidos:0:12\npedidos:1:30\n")
            }
        });
        host.on_process(backend::BROKERS, |_, _| {
            ok("b1:9092 (id: 1 rack: null) -> (\nb2:9093 (id: 2 rack: r1) -> (\n")
        });
        host.on_process(backend::QUORUM, |_, _| {
            ok("ClusterId: abc\nLeaderId: 1\nLeaderEpoch: 4\nHighWatermark: 99\nCurrentVoters: [1,2]\n")
        });
        host.on_process(backend::CONSUMER, |_, _| ok(""));
        host.on_process(backend::PRODUCER, |_, _| ok(""));
        host
    }

    fn active_connection(host: &MockHost) -> Connection {
        let _ = host;
        let mut p = Connection::new("local", "Local");
        p.bootstrap_servers = "localhost:9092".into();
        config::upsert(p.clone()).unwrap();
        config::set_active(&p.id);
        p
    }

    #[test]
    fn reload_builds_the_explorer_from_cli_output() {
        let host = host_with_cluster();
        active_connection(&host);
        let mut plugin = Kafka::default();
        plugin.activate(&host.context("brunornasc.kafka")).unwrap();
        plugin.reload().unwrap();
        assert_eq!(plugin.topic_names, vec!["clientes", "pedidos"]);
        assert_eq!(plugin.topics.len(), 2);
        assert_eq!(plugin.groups, vec!["g1"]);
        assert_eq!(plugin.cluster.brokers.len(), 2);
        assert_eq!(plugin.cluster.controller_id, Some(1));
        assert!(plugin.status.contains("2 topics"));
    }

    #[test]
    fn explorer_tree_has_connections_topics_brokers_and_groups() {
        let host = host_with_cluster();
        active_connection(&host);
        let mut plugin = Kafka::default();
        plugin.activate(&host.context("brunornasc.kafka")).unwrap();
        plugin.expanded.insert("cat:local:topics".into());
        let view = plugin.render_explorer();
        let Body::Tree(items) = view.body else {
            panic!("expected tree")
        };
        assert!(items.iter().any(|i| i.key == "conn:local"));
        assert!(items.iter().any(|i| i.key == "cat:local:topics"));
        assert!(items.iter().any(|i| i.key == "topic:local:pedidos"));
        assert!(items.iter().any(|i| i.key == "cat:local:brokers"));
        assert!(items.iter().any(|i| i.key == "cat:local:groups"));
    }

    #[test]
    fn connections_are_tested_before_being_saved() {
        let host = host_with_cluster();
        let mut plugin = Kafka::default();
        plugin.draft = Some(Connection::new("local", "Local"));
        plugin.draft.as_mut().unwrap().bootstrap_servers = "localhost:9092".into();
        let out = plugin
            .command("connection-save", &host.invocation(), None)
            .unwrap()
            .unwrap();
        assert!(out.contains("saved and activated"));
        assert_eq!(config::active_id().as_deref(), Some("local"));
        assert!(
            host.process_runs()
                .iter()
                .any(|r| r.args.contains(&"--list".to_owned()))
        );
    }

    #[test]
    fn command_results_update_the_status_chip() {
        let host = host_with_cluster();
        let mut plugin = Kafka::default();
        plugin.activate(&host.context("brunornasc.kafka")).unwrap();
        assert_eq!(host.status_items()["kafka"].text, "Kafka •");

        plugin.draft = Some(Connection::new("local", "Local"));
        plugin.draft.as_mut().unwrap().bootstrap_servers = "localhost:9092".into();
        let out = plugin
            .run_command("connection-save", &host.invocation(), None)
            .unwrap()
            .unwrap();
        let saved = &host.status_items()["kafka"];
        assert_eq!(saved.text, "Kafka ✓");
        assert!(
            saved
                .tooltip
                .as_deref()
                .unwrap_or("")
                .contains("saved and activated"),
            "{saved:?}"
        );
        assert_eq!(plugin.status, out);

        let failed = plugin.run_command("not-a-command", &host.invocation(), None);
        assert!(failed.is_err());
        let failed = &host.status_items()["kafka"];
        assert_eq!(failed.text, "Kafka ✗");
        assert!(
            failed
                .tooltip
                .as_deref()
                .unwrap_or("")
                .contains("Unknown command"),
            "{failed:?}"
        );
    }

    #[test]
    fn the_connections_test_button_tests_the_draft_without_saving() {
        let host = host_with_cluster();
        let mut plugin = Kafka::default();
        plugin.draft = Some(Connection::new("", "New cluster"));
        plugin.draft.as_mut().unwrap().bootstrap_servers = "localhost:9092".into();
        let out = plugin
            .command("connection-test-draft", &host.invocation(), None)
            .unwrap()
            .unwrap();
        assert!(out.contains("Connection OK"), "{out}");
        assert!(
            config::load_profiles().is_empty(),
            "testing must not save the draft"
        );
    }

    #[test]
    fn deleting_a_topic_asks_for_confirmation() {
        let host = host_with_cluster();
        active_connection(&host);
        let mut plugin = Kafka::default();
        plugin.activate(&host.context("brunornasc.kafka")).unwrap();
        host.answer_next_prompt(Some(&["confirm"]));
        // confirm() consome uma resposta; sem ela o comando é cancelado.
        let result = plugin.command("topic-delete", &host.invocation(), Some("pedidos"));
        assert!(result.is_ok(), "confirmado: {:?}", result);
        assert!(
            host.process_runs()
                .iter()
                .any(|r| r.args.contains(&"--delete".to_owned()))
        );
    }

    #[test]
    fn consume_updates_the_message_buffer_and_prefs() {
        let host = host_with_cluster();
        active_connection(&host);
        let record = format!(
            "CreateTime:1000{0}Partition:0{0}Offset:7{0}NO_HEADERS{0}k{0}v{1}",
            model::FIELD_SEP,
            model::RECORD_SEP
        );
        host.on_process(backend::CONSUMER, move |_, _| ok(&record));
        let mut plugin = Kafka::default();
        plugin.activate(&host.context("brunornasc.kafka")).unwrap();
        let count = plugin
            .command("consume", &host.invocation(), Some("pedidos"))
            .unwrap();
        assert!(count.unwrap().contains("1 messages read"));
        assert_eq!(plugin.messages.records.len(), 1);
        assert_eq!(plugin.messages.records[0].key.as_deref(), Some("k"));
        assert_eq!(plugin.prefs.topic, "pedidos");
    }

    #[test]
    fn html_page_actions_consume_and_publish() {
        let host = host_with_cluster();
        active_connection(&host);
        let mut plugin = Kafka::default();
        plugin.activate(&host.context("brunornasc.kafka")).unwrap();
        plugin
            .handle_messages(
                ViewEvent::Message(
                    json!({"action": "prefs", "topic": "pedidos", "limit": 5}).to_string(),
                ),
                &host.invocation(),
            )
            .unwrap();
        assert_eq!(plugin.prefs.topic, "pedidos");
        plugin.handle_messages(ViewEvent::Message(
            json!({"action": "publish", "topic": "pedidos", "key": "k", "value": "v", "headers": "trace=abc"}).to_string(),
        ), &host.invocation()).unwrap();
        assert!(
            host.process_runs()
                .iter()
                .any(|r| r.program == backend::PRODUCER)
        );
    }

    #[test]
    fn format_choices_are_shipped_to_the_html_and_saved_per_topic() {
        let host = host_with_cluster();
        active_connection(&host);
        let mut plugin = Kafka::default();
        plugin.activate(&host.context("brunornasc.kafka")).unwrap();
        let state: Value = serde_json::from_str(&plugin.messages_state()).unwrap();
        assert!(
            state["formats"]
                .as_array()
                .unwrap()
                .contains(&json!("KafkaProtobufSerializer"))
        );
        assert_eq!(state["topicFormats"].as_object().unwrap().len(), 0);

        plugin
            .handle_messages(
                ViewEvent::Message(
                    json!({"action": "prefs", "topic": "pedidos", "serializer": "ByteArraySerializer", "deserializer": "IntegerSerializer"}).to_string(),
                ),
                &host.invocation(),
            )
            .unwrap();
        let chosen = config::formats_for("pedidos");
        assert_eq!(chosen.serializer, "ByteArraySerializer");
        assert_eq!(chosen.deserializer, "IntegerSerializer");
        let state: Value = serde_json::from_str(&plugin.messages_state()).unwrap();
        assert_eq!(
            state["topicFormats"]["pedidos"]["serializer"],
            "ByteArraySerializer"
        );
        assert_eq!(
            state["topicFormats"]["pedidos"]["deserializer"],
            "IntegerSerializer"
        );
        assert!(PAGE.contains("id=\"format\""));
        assert!(PAGE.contains("id=\"pubFormat\""));
    }

    #[test]
    fn topics_table_shows_the_chosen_formats() {
        let host = host_with_cluster();
        active_connection(&host);
        config::set_format("pedidos", "serializer", "ByteArraySerializer").unwrap();
        config::set_format("pedidos", "deserializer", "IntegerSerializer").unwrap();
        let mut plugin = Kafka::default();
        plugin.activate(&host.context("brunornasc.kafka")).unwrap();
        plugin.reload().unwrap();
        let view = plugin.render_topics(None);
        let Body::Other { kind, data } = view.body else {
            panic!("expected sortable table")
        };
        assert_eq!(kind, "table", "{kind}");
        let table: Value = serde_json::from_str(&data).unwrap();
        let columns = table["columns"].as_array().unwrap();
        assert!(columns.iter().any(|c| c.as_str() == Some("Serializer")));
        assert!(columns.iter().any(|c| c.as_str() == Some("Deserializer")));
        let row = table["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r[0].as_str() == Some("pedidos"))
            .unwrap();
        assert_eq!(row[5].as_str(), Some("ByteArraySerializer"));
        assert_eq!(row[6].as_str(), Some("IntegerSerializer"));
    }

    #[test]
    fn details_show_partitions_offsets_and_a_live_log() {
        let host = host_with_cluster();
        active_connection(&host);
        let mut plugin = Kafka::default();
        plugin.activate(&host.context("brunornasc.kafka")).unwrap();
        plugin
            .inspect_topic(&config::active().unwrap(), "pedidos")
            .unwrap();
        let tabs = plugin
            .detail(EXPLORER, "topic:local:pedidos", &host.invocation())
            .unwrap();
        assert!(tabs.iter().any(|t| t.id == "details"));
        assert!(tabs.iter().any(|t| t.id == "partitions"));
        assert!(tabs.iter().any(|t| t.id == "live"));
        let Pane::Text(text) = &tabs[1].body else {
            panic!("expected text")
        };
        assert!(text.contains("Start offset"));
        assert!(text.contains("12"));
    }

    #[test]
    fn the_ui_follows_the_ide_language() {
        let host = host_with_cluster();
        active_connection(&host);
        host.set_locale("pt-BR");
        let mut plugin = Kafka::default();
        plugin.activate(&host.context("brunornasc.kafka")).unwrap();
        plugin.reload().unwrap();
        assert!(plugin.status.contains("2 tópicos"), "{}", plugin.status);
        let state: Value = serde_json::from_str(&plugin.messages_state()).unwrap();
        assert!(
            state["i18n"].get("Refresh").is_some(),
            "HTML catalog is shipped"
        );
        assert_eq!(state["i18n"]["Refresh"], "Atualizar");

        // Forcing en-US keeps the plugin's own text English even on a pt-BR IDE.
        host.set_setting("language", r#""en-US""#);
        plugin.reload().unwrap();
        assert!(plugin.status.contains("2 topics"), "{}", plugin.status);
        let state: Value = serde_json::from_str(&plugin.messages_state()).unwrap();
        assert!(state["i18n"].as_object().unwrap().is_empty());
    }

    #[test]
    fn install_view_offers_download_and_version_catalogue() {
        let host = host_with_cluster();
        let mut plugin = Kafka::default();
        plugin.activate(&host.context("brunornasc.kafka")).unwrap();
        let view = plugin.render_install();
        assert!(view.toolbar.iter().any(|b| b.id == "install-backend"));
        assert!(view.status.unwrap_or_default().contains("capabilities"));
    }

    #[test]
    fn connections_form_never_exposes_a_password_field() {
        let host = host_with_cluster();
        active_connection(&host);
        let plugin = Kafka::default();
        let view = plugin.render_connections();
        let Body::Form(fields) = view.body else {
            panic!("expected form")
        };
        assert!(fields.iter().all(|f| !f.key.contains("password")));
        assert!(fields.iter().any(|f| f.key == "client_properties"));
        assert!(fields.iter().any(|f| f.key == "sasl_jaas_config"));
        assert!(fields.iter().any(|f| f.key == "security_protocol"));
        assert!(view.toolbar.iter().any(|b| b.id == "connection-test-draft"));
    }
}
