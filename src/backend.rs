//! Ponte com a distribuição Apache Kafka.
//!
//! O WASM não tem sockets TCP, então todas as operações usam os scripts da
//! distribuição via `host::run_process` (capacidade `process:<programa>`).
//! Os sinalizadores mudaram entre Kafka 3.x (`--consumer.config`,
//! `--consumer-property`, `--property`) e 4.x (`--command-config`,
//! `--command-property`, `--formatter-property`): `execute` tenta a forma
//! moderna e cai para a antiga quando a CLI rejeita uma opção.
//!
//! Credenciais nunca aparecem em `argv`: o arquivo `--command-config` é
//! referenciado apenas pelo caminho (quando o usuário informa um arquivo) ou
//! escrito no diretório privado do plugin a partir do JAAS inline, e a saída
//! dos processos é redigida.

use crate::config::{self, Connection};
use crate::i18n;
use crate::model::{
    self, ClusterInfo, FIELD_SEP, GroupLag, HEADER_KV, HEADER_SEP, MessageRecord, NULL_MARKER,
    QuorumInfo, RECORD_SEP, Topic,
};
use openidea_plugin::details::ProcessLog;
use openidea_plugin::{Error, HttpRequest, ProcessOutput, host};

pub const TOPICS: &str = "kafka-topics.sh";
pub const CONSUMER: &str = "kafka-console-consumer.sh";
pub const PRODUCER: &str = "kafka-console-producer.sh";
pub const GROUPS: &str = "kafka-consumer-groups.sh";
pub const OFFSETS: &str = "kafka-get-offsets.sh";
pub const RUN_CLASS: &str = "kafka-run-class.sh";
pub const BROKERS: &str = "kafka-broker-api-versions.sh";
pub const QUORUM: &str = "kafka-metadata-quorum.sh";
pub const KCAT: &str = "kcat";
pub const CURL: &str = "curl";
pub const TAR: &str = "tar";
pub const JAVA: &str = "java";

/// Versões conhecidas quando o catálogo online não está disponível.
pub const BUILTIN_VERSIONS: [&str; 8] = [
    "4.4.0", "4.3.2", "4.2.2", "4.1.2", "3.9.1", "3.8.1", "3.7.2", "3.6.2",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tool {
    Topics,
    Consumer,
    Producer,
    Groups,
    Offsets,
    Brokers,
    Quorum,
}

impl Tool {
    fn program(self) -> &'static str {
        match self {
            Self::Topics => TOPICS,
            Self::Consumer => CONSUMER,
            Self::Producer => PRODUCER,
            Self::Groups => GROUPS,
            Self::Offsets => OFFSETS,
            Self::Brokers => BROKERS,
            Self::Quorum => QUORUM,
        }
    }

    /// Classe Java executada pelo script da distribuição. Kafka 3.7+ e 4.x
    /// usam pacotes `org.apache.kafka.tools`; 3.6 ainda usa `kafka.*`.
    fn main_class(self, version: &str) -> &'static str {
        let mut parts = version.split('.');
        let major = parts
            .next()
            .and_then(|p| p.parse::<u64>().ok())
            .unwrap_or(4);
        let minor = parts
            .next()
            .and_then(|p| p.parse::<u64>().ok())
            .unwrap_or(0);
        let new = major > 3 || (major == 3 && minor >= 7);
        match (self, new) {
            (Self::Topics, true) => "org.apache.kafka.tools.TopicCommand",
            (Self::Topics, false) => "kafka.tools.TopicCommand",
            (Self::Consumer, true) => "org.apache.kafka.tools.consumer.ConsoleConsumer",
            (Self::Consumer, false) => "kafka.tools.ConsoleConsumer",
            (Self::Producer, true) => "org.apache.kafka.tools.ConsoleProducer",
            (Self::Producer, false) => "kafka.tools.ConsoleProducer",
            (Self::Groups, true) => "org.apache.kafka.tools.consumer.group.ConsumerGroupCommand",
            (Self::Groups, false) => "kafka.admin.ConsumerGroupCommand",
            (Self::Offsets, true) => "org.apache.kafka.tools.GetOffsetShell",
            (Self::Offsets, false) => "kafka.tools.GetOffsetShell",
            (Self::Brokers, true) => "org.apache.kafka.tools.BrokerApiVersionsCommand",
            (Self::Brokers, false) => "kafka.admin.BrokerApiVersionsCommand",
            (Self::Quorum, _) => "org.apache.kafka.tools.MetadataQuorumCommand",
        }
    }
}

// ------------------------------------------------------------- execução

fn friendly_denied(cap: &str) -> Error {
    if cap.contains("kcat") {
        return i18n::t(
            "The process:kcat capability is not granted. Install kcat with your system package manager and enable it in Settings › Plugins › Kafka › Capabilities (or publish without a target partition)."
        )
        .into();
    }
    if cap.contains("java") {
        return i18n::t(
            "The process:java capability is not granted. Java is on the PATH, but OpenIDEA still needs it approved in Settings › Plugins › Kafka › Capabilities to run the installed Kafka distribution.",
        )
        .into();
    }
    i18n::tf(
        "Capability {cap} is not granted. In Settings › Plugins › Kafka › Capabilities, enable it and point the program to the script of the Apache Kafka distribution. If you installed the CLI after allowing it, remove and re-add the capability: OpenIDEA resolves the program when the permission is granted.",
        &[("cap", cap)],
    )
    .into()
}

fn raw(program: &str, args: &[String], stdin: Option<&str>) -> Result<ProcessOutput, Error> {
    host::run_process(program, args, stdin)
}

fn friendly(error: Error) -> Error {
    match error {
        Error::Denied(cap) => friendly_denied(&cap),
        Error::Failed(msg) if msg.starts_with(JAVA) && msg.contains("timed out") => {
            i18n::t(
                "Timed out talking to the cluster. Check the bootstrap servers, the network and request.timeout.ms in the client properties.",
            )
            .into()
        }
        other => other,
    }
}

/// Transforma status de saída e stderr em mensagens claras, sem segredos.
fn classify(output: &ProcessOutput, program: &str) -> Option<Error> {
    if output.status == Some(0) {
        return None;
    }
    let text = format!("{}\n{}", output.stdout, output.stderr);
    let lower = config::redact(&text).to_ascii_lowercase();
    let message = if lower.contains("authentication")
        || lower.contains("sasl")
        || lower.contains("login failed")
        || lower.contains("security.disable")
    {
        i18n::t(
            "Authentication failed. Check security.protocol, the SASL mechanism and the credentials (client properties file or inline JAAS).",
        )
    } else if lower.contains("authorization")
        || lower.contains("not authorized")
        || lower.contains("acl")
    {
        i18n::t(
            "The cluster refused the operation due to permissions/ACL. Check the user's grants.",
        )
    } else if lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("timeoutexception")
    {
        i18n::t(
            "Timed out talking to the cluster. Check the bootstrap servers, the network and request.timeout.ms in the client properties.",
        )
    } else if lower.contains("connection refused") || lower.contains("disconnected") {
        i18n::t(
            "Could not connect to the broker. Check the host/port and whether the cluster is up.",
        )
    } else if lower.contains("unknown_topic")
        || lower.contains("topic") && lower.contains("does not exist")
    {
        i18n::t("Topic not found in the cluster.")
    } else if lower.contains("no such file")
        || lower.contains("not found") && lower.contains("program")
    {
        i18n::tf(
            "Program {program} not found. Point the process:{program} capability to the installed script.",
            &[("program", program)],
        )
    } else {
        let detail: String = config::redact(&text)
            .lines()
            .filter(|l| !l.trim().is_empty())
            .take(12)
            .collect::<Vec<_>>()
            .join(" | ");
        {
            let status = format!("{:?}", output.status);
            i18n::tf(
                "{program} failed (status {status}): {detail}",
                &[
                    ("program", program),
                    ("status", &status),
                    ("detail", &clip(&detail, 900)),
                ],
            )
        }
    };
    Some(message.into())
}

fn unknown_option(output: &ProcessOutput) -> bool {
    if output.status == Some(0) {
        return false;
    }
    let text = format!("{}\n{}", output.stdout, output.stderr).to_ascii_lowercase();
    [
        "unrecognized option",
        "unknown option",
        "not a recognized option",
        "invalid option",
        "no such option",
        "não reconhec",
    ]
    .iter()
    .any(|p| text.contains(p))
}

fn run(program: &str, args: &[String], stdin: Option<&str>) -> Result<ProcessOutput, Error> {
    let output = raw(program, args, stdin).map_err(friendly)?;
    match classify(&output, program) {
        Some(e) => Err(e),
        None => Ok(output),
    }
}

/// Executa a distribuição baixada diretamente com `java -cp`, sem depender
/// das capacidades `process:*.sh` (que ficam presas ao programa aprovado).
/// Se `process:java` não estiver concedida, cai para o script instalado no
/// sistema, mantendo compatibilidade com quem já apontou as capacidades.
fn java_command(tool: Tool, args: &[String]) -> Option<(String, Vec<String>)> {
    let info = config::install_info()?;
    let mut out = vec![
        "-cp".into(),
        format!("{}/libs/*", info.dir.trim_end_matches('/')),
        tool.main_class(&info.version).into(),
    ];
    out.extend(args.iter().cloned());
    Some((JAVA.into(), out))
}

/// Tenta os sinalizadores do Kafka 4.x e cai para os do 3.x quando a CLI
/// rejeita uma opção. `cliFlavour` permite fixar uma das formas.
fn run_compat(
    tool: Tool,
    modern: Vec<String>,
    legacy: Vec<String>,
    stdin: Option<&str>,
) -> Result<ProcessOutput, Error> {
    let attempt = |args: &[String]| -> Result<ProcessOutput, Error> {
        if let Some((program, full)) = java_command(tool, args) {
            match raw(&program, &full, stdin) {
                Ok(out) => return Ok(out),
                // Java não aprovado: ainda é possível usar os scripts.
                Err(Error::Denied(_)) => {}
                Err(e) => return Err(friendly(e)),
            }
        }
        raw(tool.program(), args, stdin).map_err(friendly)
    };
    let program = tool.program();
    let finish = |out: ProcessOutput| match classify(&out, program) {
        Some(e) => Err(e),
        None => Ok(out),
    };
    match config::cli_flavour().as_str() {
        "kafka4" => return finish(attempt(&modern)?),
        "kafka3" => return finish(attempt(&legacy)?),
        _ => {}
    }
    let first = attempt(&modern)?;
    if unknown_option(&first) {
        let second = attempt(&legacy)?;
        return finish(second);
    }
    finish(first)
}

fn execute(
    c: &Connection,
    tool: Tool,
    specific: impl Fn(bool) -> Result<Vec<String>, Error>,
    stdin: Option<&str>,
) -> Result<ProcessOutput, Error> {
    let modern = build(c, tool, true, specific(true)?)?;
    let legacy = build(c, tool, false, specific(false)?)?;
    // Um task mantém a chamada viva por até 60 s: a JVM + SASL costumam
    // demorar mais que o orçamento padrão de 5 s do host.
    let task = host::begin_task(
        &i18n::tf("Kafka: {program}", &[("program", tool.program())]),
        true,
    );
    let result = run_compat(tool, modern, legacy, stdin);
    if task.is_cancelled() {
        return Err(Error::Cancelled);
    }
    result
}

fn build(
    c: &Connection,
    tool: Tool,
    modern: bool,
    specific: Vec<String>,
) -> Result<Vec<String>, Error> {
    Ok(strings(&["--bootstrap-server", &c.bootstrap_servers])
        .into_iter()
        .chain(specific)
        .chain(client_props(c, tool, modern)?)
        .collect())
}

/// Hash estável para o nome do arquivo gerado a partir do JAAS inline.
fn stable_hash(text: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in text.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// JAAS pode vir quebrado em várias linhas; em um arquivo `.properties` ele
/// precisa ser uma única linha. Barras invertidas são escapadas para o
/// carregador de propriedades Java não alterar a senha.
fn one_line_jaas(text: &str) -> String {
    let mut out = String::new();
    let mut after_newline = false;
    for ch in text.trim().chars() {
        if after_newline {
            if ch.is_whitespace() {
                continue;
            }
            if !out.ends_with(' ') && !out.ends_with('\t') {
                out.push(' ');
            }
            after_newline = false;
        }
        if matches!(ch, '\r' | '\n') {
            after_newline = true;
        } else if ch == '\\' {
            out.push_str("\\\\");
        } else {
            out.push(ch);
        }
    }
    out
}

/// Caminho do `--command-config`: o arquivo do usuário, ou um arquivo no
/// diretório privado do plugin escrito a partir do JAAS inline. Nenhum segredo
/// aparece em `argv` — apenas o caminho.
fn properties_path(c: &Connection) -> Result<Option<String>, Error> {
    if !c.client_properties.trim().is_empty() {
        return Ok(Some(c.client_properties.clone()));
    }
    if c.sasl_jaas_config.trim().is_empty() {
        return Ok(None);
    }
    let dir = host::fs::plugin_dir()?;
    let hash = stable_hash(&format!(
        "{}|{}|{}",
        c.id, c.security_protocol, c.sasl_jaas_config
    ));
    let path = format!("{dir}/kafka-client-{hash:016x}.properties");
    let body = format!(
        "security.protocol={}\nsasl.mechanism={}\nsasl.jaas.config={}\n",
        c.security_protocol,
        c.sasl_mechanism,
        one_line_jaas(&c.sasl_jaas_config)
    );
    host::fs::write_file(&path, &body).map_err(|e| {
        Error::Failed(i18n::tf(
            "Could not write the inline JAAS properties file: {error}",
            &[("error", &e.to_string())],
        ))
    })?;
    Ok(Some(path))
}

/// `--command-config` / `--*.config` e propriedades extras não sensíveis.
fn client_props(c: &Connection, tool: Tool, modern: bool) -> Result<Vec<String>, Error> {
    let mut out = Vec::new();
    let config_flag = match (tool, modern) {
        (Tool::Consumer, false) => "--consumer.config",
        (Tool::Producer, false) => "--producer.config",
        _ => "--command-config",
    };
    if let Some(path) = properties_path(c)? {
        out.push(config_flag.into());
        out.push(path);
    }
    let property_flag = match (tool, modern) {
        (Tool::Consumer, false) => "--consumer-property",
        (Tool::Producer, false) => "--producer-property",
        (Tool::Topics | Tool::Groups | Tool::Offsets | Tool::Brokers | Tool::Quorum, false) => "",
        _ => "--command-property",
    };
    if !property_flag.is_empty() {
        for (key, value) in c.java_properties() {
            out.push(property_flag.into());
            out.push(format!("{key}={value}"));
        }
    }
    Ok(out)
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

pub fn clip(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_owned();
    }
    let mut out: String = text.chars().take(max).collect();
    out.push('…');
    out
}

// ------------------------------------------------------------- operações

/// Testa a conexão antes de salvar (lista tópicos).
pub fn test_connection(c: &Connection) -> Result<usize, Error> {
    c.validate()?;
    let out = execute(c, Tool::Topics, |_| Ok(strings(&["--list"])), None)?;
    Ok(model::parse_topic_list(&out.stdout).len())
}

pub fn list_topics(c: &Connection) -> Result<Vec<String>, Error> {
    let out = execute(c, Tool::Topics, |_| Ok(strings(&["--list"])), None)?;
    Ok(model::parse_topic_list(&out.stdout))
}

/// Detalhes de um tópico (ou de todos quando `topic` é `None`).
pub fn describe_topics(c: &Connection, topic: Option<&str>) -> Result<Vec<Topic>, Error> {
    let name = topic.unwrap_or("").to_owned();
    let out = execute(
        c,
        Tool::Topics,
        |_| {
            Ok({
                let mut args = vec!["--describe".to_owned()];
                if !name.is_empty() {
                    args.extend(strings(&["--topic", &name]));
                }
                args
            })
        },
        None,
    )?;
    let mut topics = model::parse_topic_describe(&out.stdout);
    // `--describe` sem tópico pode não trazer todas as configs; a lista evita
    // que um tópico sem partições declaradas desapareça.
    if topic.is_none()
        && let Ok(names) = list_topics(c)
    {
        for n in names {
            if !topics.iter().any(|t| t.name == n) {
                topics.push(Topic {
                    name: n,
                    ..Topic::default()
                });
            }
        }
        topics.sort_by(|a, b| a.name.cmp(&b.name));
    }
    Ok(topics)
}

pub fn create_topic(
    c: &Connection,
    name: &str,
    partitions: u32,
    replication_factor: u32,
    configs: &[(String, String)],
) -> Result<(), Error> {
    if name.trim().is_empty() || partitions == 0 || replication_factor == 0 {
        return Err(i18n::t("Enter a name, partitions (> 0) and replication factor (> 0).").into());
    }
    let mut specific = strings(&[
        "--create",
        "--topic",
        name,
        "--partitions",
        &partitions.to_string(),
        "--replication-factor",
        &replication_factor.to_string(),
    ]);
    for (key, value) in configs {
        if config::is_secret_key(key) {
            return Err(i18n::t("Topic configurations cannot contain secrets.").into());
        }
        specific.extend(strings(&["--config", &format!("{key}={value}")]));
    }
    let out = execute(c, Tool::Topics, move |_| Ok(specific.clone()), None)?;
    let _ = out;
    Ok(())
}

pub fn delete_topic(c: &Connection, name: &str) -> Result<(), Error> {
    if name.trim().is_empty() {
        return Err(i18n::t("Choose a topic.").into());
    }
    let name = name.to_owned();
    execute(
        c,
        Tool::Topics,
        move |_| Ok(strings(&["--delete", "--topic", &name])),
        None,
    )?;
    Ok(())
}

/// Offsets inicial/final por partição. Tenta `kafka-get-offsets.sh` e cai para
/// `kafka-run-class.sh kafka.tools.GetOffsetShell` quando o script não existe.
pub fn offsets(c: &Connection, topic: &str, earliest: bool) -> Result<Vec<(i32, i64)>, Error> {
    let topic = topic.to_owned();
    let time = if earliest { "-2" } else { "-1" };
    let result = execute(
        c,
        Tool::Offsets,
        |_| Ok(strings(&["--topic", &topic, "--time", time])),
        None,
    );
    let out = match result {
        Ok(out) => out,
        Err(first) => {
            // Forma antiga (Kafka 2.x/3.x sem o script novo).
            let mut args = strings(&[
                "kafka.tools.GetOffsetShell",
                "--broker-list",
                &c.bootstrap_servers,
                "--topic",
                &topic,
                "--time",
                time,
            ]);
            if let Some(path) = properties_path(c)? {
                args.extend(strings(&["--command-config", &path]));
            }
            match raw(RUN_CLASS, &args, None) {
                Ok(out) if out.status == Some(0) => out,
                _ => return Err(first),
            }
        }
    };
    Ok(model::parse_offsets(&out.stdout)
        .into_iter()
        .filter(|(t, _, _)| t == &topic)
        .map(|(_, p, o)| (p, o))
        .collect())
}

/// Preenche `earliest`/`latest` das partições de um tópico (melhor esforço).
pub fn fill_offsets(c: &Connection, topic: &mut Topic) -> Vec<String> {
    let mut notes = Vec::new();
    match (
        offsets(c, &topic.name, true),
        offsets(c, &topic.name, false),
    ) {
        (Ok(first), Ok(last)) => {
            for p in &mut topic.details {
                p.earliest = first.iter().find(|(id, _)| *id == p.id).map(|(_, o)| *o);
                p.latest = last.iter().find(|(id, _)| *id == p.id).map(|(_, o)| *o);
            }
        }
        (Err(e), _) | (_, Err(e)) => notes.push(e.to_string()),
    }
    notes
}

pub fn list_groups(c: &Connection) -> Result<Vec<String>, Error> {
    let out = execute(c, Tool::Groups, |_| Ok(strings(&["--list"])), None)?;
    Ok(model::parse_group_list(&out.stdout))
}

pub fn describe_group(c: &Connection, group: &str) -> Result<Vec<GroupLag>, Error> {
    let group = group.to_owned();
    let out = execute(
        c,
        Tool::Groups,
        move |_| Ok(strings(&["--describe", "--group", &group])),
        None,
    )?;
    Ok(model::parse_group_describe(&out.stdout))
}

/// Brokers, controlador e metadados do cluster (KRaft quando disponível).
pub fn cluster_info(c: &Connection) -> (ClusterInfo, Vec<String>) {
    let mut info = ClusterInfo::default();
    let mut notes = Vec::new();
    match execute(c, Tool::Brokers, |_| Ok(Vec::new()), None) {
        Ok(out) => info.brokers = model::parse_broker_versions(&out.stdout),
        Err(e) => notes.push(e.to_string()),
    }
    match execute(
        c,
        Tool::Quorum,
        |_| Ok(strings(&["describe", "--status"])),
        None,
    ) {
        Ok(out) => {
            let quorum: Option<QuorumInfo> = model::parse_quorum(&out.stdout);
            info.controller_id = quorum.as_ref().and_then(|q| q.leader_id);
            info.quorum = quorum;
        }
        Err(e) => notes.push(i18n::tf(
            "Controller metadata unavailable: {error}",
            &[("error", &e.to_string())],
        )),
    }
    if let Some(id) = info.controller_id {
        for b in &mut info.brokers {
            if b.id == id {
                b.status = i18n::t("controller");
            }
        }
    }
    (info, notes)
}

// ------------------------------------------------------------- mensagens

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConsumeOptions {
    pub topic: String,
    pub partition: Option<i32>,
    /// `beginning`, `end`, `offset` ou `timestamp`.
    pub start: String,
    pub offset: String,
    pub limit: u32,
    pub timeout_ms: u32,
    /// Quando `true`, não limita mensagens nem timeout (log ao vivo).
    pub live: bool,
}

fn formatter_args(flag: &str, properties: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for p in properties {
        out.push(flag.to_owned());
        out.push(p.clone());
    }
    out
}

/// Leitura estruturada: separadores de controle preservam valores com tabs,
/// vírgulas e quebras de linha.
fn formatter_props(modern: bool) -> Vec<String> {
    let flag = if modern {
        "--formatter-property"
    } else {
        "--property"
    };
    let properties = [
        "print.timestamp=true".into(),
        "print.partition=true".into(),
        "print.offset=true".into(),
        "print.headers=true".into(),
        "print.key=true".into(),
        "print.value=true".into(),
        format!("key.separator={FIELD_SEP}"),
        format!("line.separator={RECORD_SEP}"),
        format!("headers.separator={HEADER_SEP}"),
        format!("null.literal={NULL_MARKER}"),
    ];
    formatter_args(flag, &properties)
}

/// Log ao vivo: formato legível para humanos (a aba de log não é parseada).
fn live_formatter_props(modern: bool) -> Vec<String> {
    let flag = if modern {
        "--formatter-property"
    } else {
        "--property"
    };
    let properties = [
        "print.timestamp=true".into(),
        "print.partition=true".into(),
        "print.offset=true".into(),
        "print.headers=true".into(),
        "print.key=true".into(),
        "print.value=true".into(),
        "key.separator= | ".into(),
        "headers.separator=, ".into(),
        "null.literal=∅".into(),
    ];
    formatter_args(flag, &properties)
}

/// Argumentos do console consumer. `live` remove `--max-messages`/`--timeout-ms`.
pub fn consume_args(
    c: &Connection,
    opts: &ConsumeOptions,
    modern: bool,
) -> Result<Vec<String>, Error> {
    let mut args = strings(&["--topic", &opts.topic]);
    if let Some(partition) = opts.partition {
        args.extend(strings(&["--partition", &partition.to_string()]));
        let offset = match opts.start.as_str() {
            "beginning" => "earliest".into(),
            "offset" => opts.offset.clone(),
            _ => "latest".into(),
        };
        args.extend(strings(&["--offset", &offset]));
    } else if opts.start == "beginning" {
        args.push("--from-beginning".into());
    }
    if !opts.live {
        args.extend(strings(&["--max-messages", &opts.limit.to_string()]));
        args.extend(strings(&["--timeout-ms", &opts.timeout_ms.to_string()]));
    }
    args.extend(if opts.live {
        live_formatter_props(modern)
    } else {
        formatter_props(modern)
    });
    build(c, Tool::Consumer, modern, args)
}

pub fn consume(c: &Connection, opts: &ConsumeOptions) -> Result<Vec<MessageRecord>, Error> {
    if opts.topic.trim().is_empty() {
        return Err(i18n::t("Choose a topic to consume.").into());
    }
    if opts.start == "offset" {
        if opts.partition.is_none() {
            return Err(
                i18n::t("To consume from a specific offset, also specify the partition.").into(),
            );
        }
        if opts.offset.trim().parse::<i64>().is_err() {
            return Err(i18n::t("Invalid start offset: use a non-negative integer.").into());
        }
    }
    let opts = opts.clone();
    let out = execute(
        c,
        Tool::Consumer,
        |modern| consume_args(c, &opts, modern),
        None,
    )?;
    Ok(model::parse_messages(&out.stdout, &opts.topic))
}

/// Log nativo do IDE que acompanha o tópico em tempo real (sem polling).
pub fn live_log(c: &Connection, opts: &ConsumeOptions) -> Result<ProcessLog, Error> {
    let mut live = opts.clone();
    live.live = true;
    let modern = config::cli_flavour() != "kafka3";
    let args = consume_args(c, &live, modern)?;
    let (program, full) =
        java_command(Tool::Consumer, &args).unwrap_or_else(|| (CONSUMER.into(), args.clone()));
    let refs: Vec<&str> = full.iter().map(String::as_str).collect();
    let mut log = ProcessLog::new(&program, &refs);
    if live.partition.is_none() && live.start != "beginning" {
        log = log.toggle(
            "from-beginning",
            &i18n::t("From the beginning"),
            &["--from-beginning"],
        );
    }
    Ok(log)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PublishMessage {
    pub topic: String,
    pub key: Option<String>,
    pub value: String,
    pub headers: Vec<(String, String)>,
    pub partition: Option<i32>,
}

/// Linha de stdin do console producer: `headers<US>chave<US>valor`.
/// Com `--property parse.headers/parse.key` e separadores de controle.
pub fn publish_input(msg: &PublishMessage) -> String {
    let headers = msg
        .headers
        .iter()
        .map(|(k, v)| format!("{k}{HEADER_KV}{v}"))
        .collect::<Vec<_>>()
        .join(&HEADER_SEP.to_string());
    let key = msg.key.clone().unwrap_or_else(|| NULL_MARKER.into());
    if msg.headers.is_empty() && msg.key.is_none() {
        return format!("{}\n", msg.value);
    }
    format!("{headers}{FIELD_SEP}{key}{FIELD_SEP}{}\n", msg.value)
}

/// Sinalizadores do leitor de linhas do console producer.
pub fn producer_reader_props(msg: &PublishMessage) -> Vec<String> {
    let mut props = vec![
        format!("key.separator={FIELD_SEP}"),
        format!("headers.delimiter={FIELD_SEP}"),
        format!("headers.separator={HEADER_SEP}"),
        format!("headers.key.separator={HEADER_KV}"),
        format!("null.marker={NULL_MARKER}"),
    ];
    if !msg.headers.is_empty() {
        props.push("parse.headers=true".into());
    }
    if !msg.headers.is_empty() || msg.key.is_some() {
        props.push("parse.key=true".into());
    }
    props
}

pub fn publish(c: &Connection, msg: &PublishMessage) -> Result<(), Error> {
    if msg.topic.trim().is_empty() {
        return Err(i18n::t("Choose a topic to publish to.").into());
    }
    if msg.partition.is_some() {
        return publish_with_kcat(c, msg);
    }
    let msg = msg.clone();
    execute(
        c,
        Tool::Producer,
        |_| {
            Ok({
                let mut args = strings(&["--topic", &msg.topic, "--sync"]);
                for prop in producer_reader_props(&msg) {
                    // O produtor usa `--property` (3.x) ou `--reader-property` (4.2);
                    // `--property` continua aceito nas duas com um aviso.
                    args.extend(strings(&["--property", &prop]));
                }
                args
            })
        },
        Some(&publish_input(&msg)),
    )?;
    Ok(())
}

/// `kcat -P -p <partição>` é a única forma de fixar a partição de destino com
/// as ferramentas de linha de comando padrão.
fn publish_with_kcat(c: &Connection, msg: &PublishMessage) -> Result<(), Error> {
    let partition = msg.partition.unwrap_or(0);
    let mut args = strings(&[
        "-b",
        &c.bootstrap_servers,
        "-P",
        "-t",
        &msg.topic,
        "-p",
        &partition.to_string(),
    ]);
    if !c.kcat_config.is_empty() {
        args.extend(strings(&["-F", &c.kcat_config]));
    }
    if let Some(key) = &msg.key {
        args.extend(strings(&["-k", key]));
    }
    for (key, value) in &msg.headers {
        args.extend(strings(&["-H", &format!("{key}={value}")]));
    }
    for (key, value) in c.kcat_properties() {
        args.extend(strings(&["-X", &format!("{key}={value}")]));
    }
    match raw(KCAT, &args, Some(&msg.value)) {
        Err(Error::Denied(_)) => Err(i18n::t(
            "The process:kcat capability is not granted. Install kcat with your system package manager and enable it in Settings › Plugins › Kafka › Capabilities (or publish without a target partition)."
        )
        .into()),
        Err(e) => Err(e),
        Ok(out) => match classify(&out, KCAT) {
            Some(e) => Err(e),
            None => Ok(()),
        },
    }
}

// ------------------------------------------------------------- instalação

/// Catálogo de versões: índice do Apache (opcional) + lista embutida.
pub fn available_versions() -> Vec<String> {
    let mut versions: Vec<String> = BUILTIN_VERSIONS.iter().map(|s| (*s).into()).collect();
    let request =
        HttpRequest::get("https://downloads.apache.org/kafka/").header("Accept", "text/html");
    let request = HttpRequest {
        timeout_ms: Some(10_000),
        ..request
    };
    if let Ok(response) = host::fetch(&request)
        && response.status == 200
    {
        for part in response.text().split("href=\"") {
            if let Some(candidate) = part.split('"').next()
                && let Some(version) = candidate.strip_suffix('/')
                && version.split('.').count() == 3
                && version.chars().all(|c| c.is_ascii_digit() || c == '.')
                && !versions.iter().any(|v| v == version)
            {
                versions.push(version.into());
            }
        }
    }
    versions.sort_by(|a, b| compare_versions(b, a));
    versions.dedup();
    versions
}

fn compare_versions(a: &str, b: &str) -> std::cmp::Ordering {
    let nums = |s: &str| -> Vec<u64> { s.split('.').map(|p| p.parse().unwrap_or(0)).collect() };
    nums(a).cmp(&nums(b))
}

fn valid_version(version: &str) -> bool {
    version.len() <= 16
        && version.split('.').count() == 3
        && version.chars().all(|c| c.is_ascii_digit() || c == '.')
}

/// Baixa e extrai a distribuição binária do Apache Kafka em `plugin-dir`.
/// Nenhum `fs.*` é necessário: o plugin-dir é privado do plugin.
pub fn install(version: &str, force: bool) -> Result<config::InstallInfo, Error> {
    if !valid_version(version) {
        return Err(i18n::tf("Invalid version: {version}.", &[("version", version)]).into());
    }
    let dir = host::fs::plugin_dir()?;
    let root = format!("{}/kafka_2.13-{version}", dir.trim_end_matches('/'));
    let bin = format!("{root}/bin");
    if !force && host::fs::stat(&format!("{bin}/{TOPICS}")).is_ok() {
        let info = config::InstallInfo {
            version: version.into(),
            dir: root,
            bin,
        };
        config::set_install_info(&info)?;
        return Ok(info);
    }
    let url = config::download_url(version);
    let tarball = format!("{dir}/kafka_2.13-{version}.tgz");
    let task = host::begin_task(
        &i18n::tf("Kafka: installing {version}", &[("version", version)]),
        true,
    );
    task.report(
        1,
        Some(3),
        Some(&i18n::t("Downloading the Apache Kafka tarball")),
    );
    run(
        CURL,
        &strings(&[
            "-fL",
            "--retry",
            "3",
            "--connect-timeout",
            "20",
            "-o",
            &tarball,
            &url,
        ]),
        None,
    )?;
    if task.is_cancelled() {
        return Err(Error::Cancelled);
    }
    task.report(2, Some(3), Some(&i18n::t("Extracting the distribution")));
    run(TAR, &strings(&["-xzf", &tarball, "-C", &dir]), None)?;
    task.report(3, Some(3), Some(&i18n::t("Verifying the scripts")));
    let expected = format!("{bin}/{TOPICS}");
    host::fs::stat(&expected).map_err(|_| {
        i18n::tf(
            "The extraction finished, but {path} was not found. Check the downloaded tarball.",
            &[("path", &expected)],
        )
    })?;
    let info = config::InstallInfo {
        version: version.into(),
        dir: root,
        bin: bin.clone(),
    };
    config::set_install_info(&info)?;
    Ok(info)
}

#[derive(Clone, Debug, Default)]
pub struct BackendStatus {
    pub programs: Vec<(String, String)>,
    pub installed: Option<config::InstallInfo>,
    pub java: Option<String>,
}

/// Verifica quais `process:*` estão aprovados e se o Java está disponível.
pub fn check_backend() -> BackendStatus {
    let mut status = BackendStatus {
        installed: config::install_info(),
        ..BackendStatus::default()
    };
    let programs: Vec<&str> = if status.installed.is_some() {
        // Com a distribuição instalada, os scripts não são necessários.
        vec![CURL, TAR, JAVA, KCAT]
    } else {
        vec![
            CURL, TAR, TOPICS, CONSUMER, PRODUCER, GROUPS, OFFSETS, BROKERS, QUORUM, KCAT,
        ]
    };
    for program in programs {
        let result = raw(program, &strings(&["--version"]), None);
        let state = match result {
            Ok(out) => {
                let version = config::redact(&format!("{}\n{}", out.stdout, out.stderr))
                    .lines()
                    .find(|l| !l.trim().is_empty())
                    .unwrap_or(&i18n::t("available"))
                    .trim()
                    .to_owned();
                format!("OK · {}", clip(&version, 90))
            }
            Err(Error::Denied(_)) => i18n::t("not granted · re-approve after installing the CLI"),
            Err(e) => {
                let detail = clip(&e.to_string(), 90);
                i18n::tf("unavailable · {error}", &[("error", &detail)])
            }
        };
        status.programs.push((program.into(), state));
    }
    if let Ok(out) = raw(JAVA, &strings(&["-version"]), None) {
        let text = format!("{}\n{}", out.stdout, out.stderr);
        status.java = text
            .lines()
            .find(|l| l.contains("version"))
            .map(|l| clip(l.trim(), 90));
    }
    status
}

/// Caminhos dos scripts instalados, para o usuário apontar as capacidades.
pub fn installed_scripts() -> Vec<(String, String)> {
    let Some(info) = config::install_info() else {
        return Vec::new();
    };
    [
        TOPICS, CONSUMER, PRODUCER, GROUPS, OFFSETS, RUN_CLASS, BROKERS, QUORUM,
    ]
    .iter()
    .map(|p| ((*p).into(), format!("{}/{p}", info.bin)))
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Connection;

    fn connection() -> Connection {
        let mut c = Connection::new("local", "Local");
        c.bootstrap_servers = "localhost:9092".into();
        c
    }

    fn ok(stdout: &str) -> Result<ProcessOutput, Error> {
        Ok(ProcessOutput {
            status: Some(0),
            stdout: stdout.into(),
            stderr: String::new(),
        })
    }

    #[test]
    fn test_connection_lists_topics_and_passes_the_config_file() {
        let host = crate::mock_host();
        host.on_process(TOPICS, |args, _| {
            assert!(args.contains(&"--list".to_owned()));
            assert!(args.contains(&"--bootstrap-server".to_owned()));
            ok("pedidos\nclientes\n")
        });
        let n = test_connection(&connection()).unwrap();
        assert_eq!(n, 2);
        assert_eq!(host.process_runs()[0].program, TOPICS);
    }

    #[test]
    fn secure_connections_forward_the_properties_file_without_secrets() {
        let host = crate::mock_host();
        let mut c = connection();
        c.security_protocol = "SASL_SSL".into();
        c.sasl_mechanism = "SCRAM-SHA-256".into();
        c.sasl_username = "dev".into();
        c.client_properties = "/home/user/kafka.properties".into();
        c.extra_properties = vec!["request.timeout.ms=10000".into()];
        host.on_process(TOPICS, |args, _| {
            let joined = args.join(" ");
            assert!(joined.contains("--command-config /home/user/kafka.properties"));
            assert!(joined.contains("security.protocol=SASL_SSL"));
            assert!(joined.contains("sasl.mechanism=SCRAM-SHA-256"));
            assert!(joined.contains("request.timeout.ms=10000"));
            assert!(!joined.contains("password"));
            ok("t\n")
        });
        list_topics(&c).unwrap();
    }

    #[test]
    fn inline_jaas_goes_to_a_private_file_not_to_argv() {
        let host = crate::mock_host();
        let mut c = connection();
        c.security_protocol = "SASL_PLAINTEXT".into();
        c.sasl_mechanism = "PLAIN".into();
        c.sasl_jaas_config =
            "org.example.LoginModule required\n  username=\"dev\" password=\"super\\secret\";"
                .into();
        let seen = std::rc::Rc::new(std::cell::RefCell::new(String::new()));
        let capture = seen.clone();
        host.on_process(TOPICS, move |args, _| {
            let i = args.iter().position(|a| a == "--command-config").unwrap();
            *capture.borrow_mut() = args[i + 1].clone();
            ok("t\n")
        });
        list_topics(&c).unwrap();

        let path = seen.borrow().clone();
        assert!(path.starts_with(&host::fs::plugin_dir().unwrap()), "{path}");
        let body = host::fs::read_file(&path).unwrap();
        assert!(body.contains("security.protocol=SASL_PLAINTEXT"), "{body}");
        assert!(body.contains("sasl.mechanism=PLAIN"), "{body}");
        let jaas_line = body.lines().nth(2).unwrap();
        assert_eq!(
            jaas_line,
            "sasl.jaas.config=org.example.LoginModule required username=\"dev\" password=\"super\\\\secret\";",
            "multiline JAAS must become one escaped properties line: {body}"
        );
        assert!(
            host.process_runs()
                .iter()
                .all(|r| !r.args.iter().any(|a| a.contains("LoginModule"))),
            "the secret must not appear in argv"
        );
    }

    #[test]
    fn installed_distribution_runs_directly_with_java() {
        let host = crate::mock_host();
        config::set_install_info(&config::InstallInfo {
            version: "4.4.0".into(),
            dir: "/plugins/kafka_2.13-4.4.0".into(),
            bin: "/plugins/kafka_2.13-4.4.0/bin".into(),
        })
        .unwrap();
        host.on_process(JAVA, |args, _| {
            assert!(args.iter().any(|a| a == "/plugins/kafka_2.13-4.4.0/libs/*"));
            assert!(
                args.iter()
                    .any(|a| a == "org.apache.kafka.tools.TopicCommand")
            );
            assert!(args.iter().any(|a| a == "--list"));
            ok("t\n")
        });
        list_topics(&connection()).unwrap();
        assert_eq!(host.process_runs()[0].program, JAVA);
    }

    #[test]
    fn java_main_classes_follow_the_kafka_generation() {
        assert_eq!(
            Tool::Topics.main_class("4.4.0"),
            "org.apache.kafka.tools.TopicCommand"
        );
        assert_eq!(Tool::Topics.main_class("3.6.2"), "kafka.tools.TopicCommand");
        assert_eq!(
            Tool::Groups.main_class("3.6.2"),
            "kafka.admin.ConsumerGroupCommand"
        );
    }

    #[test]
    fn compat_retries_with_legacy_flags_when_the_cli_rejects_a_new_option() {
        let host = crate::mock_host();
        let calls = std::rc::Rc::new(std::cell::Cell::new(0));
        let counter = calls.clone();
        host.on_process(TOPICS, move |args, _| {
            counter.set(counter.get() + 1);
            if args.iter().any(|a| a == "--command-property") {
                Ok(ProcessOutput {
                    status: Some(1),
                    stdout: String::new(),
                    stderr: "Unrecognized option: --command-property".into(),
                })
            } else {
                ok("ok\n")
            }
        });
        let mut c = connection();
        c.extra_properties = vec!["request.timeout.ms=1000".into()];
        list_topics(&c).unwrap();
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn consume_builds_structured_formatter_flags() {
        let host = crate::mock_host();
        host.on_process(CONSUMER, |args, _| {
            let joined = args.join("|");
            assert!(joined.contains("print.headers=true"));
            assert!(joined.contains(&format!("key.separator={FIELD_SEP}")));
            assert!(joined.contains("--max-messages"));
            assert!(joined.contains("--timeout-ms"));
            ok("")
        });
        let opts = ConsumeOptions {
            topic: "pedidos".into(),
            start: "beginning".into(),
            limit: 10,
            timeout_ms: 1000,
            ..ConsumeOptions::default()
        };
        assert!(consume(&connection(), &opts).unwrap().is_empty());
    }

    #[test]
    fn an_explicit_offset_requires_a_partition_and_a_number() {
        let _host = crate::mock_host();
        let c = connection();
        let opts = ConsumeOptions {
            topic: "t".into(),
            start: "offset".into(),
            offset: "12".into(),
            ..ConsumeOptions::default()
        };
        assert!(consume(&c, &opts).is_err(), "sem partição");
        let opts = ConsumeOptions {
            partition: Some(0),
            offset: "abc".into(),
            ..opts
        };
        assert!(consume(&c, &opts).is_err(), "offset inválido");
    }

    #[test]
    fn live_log_never_limits_the_consumer() {
        let _host = crate::mock_host();
        let opts = ConsumeOptions {
            topic: "t".into(),
            ..ConsumeOptions::default()
        };
        let log = live_log(&connection(), &opts).unwrap();
        assert!(!log.args.iter().any(|a| a == "--max-messages"));
        assert!(!log.args.iter().any(|a| a == "--timeout-ms"));
    }

    #[test]
    fn publish_encodes_key_headers_and_value() {
        let host = crate::mock_host();
        host.on_process(PRODUCER, |args, stdin| {
            assert!(args.contains(&"--sync".to_owned()));
            assert!(args.contains(&"parse.headers=true".to_owned()));
            let line = stdin.unwrap();
            assert!(line.contains(&format!("trace{HEADER_KV}abc")));
            assert!(line.contains("pedido-1"));
            assert!(line.contains("{\"id\":1}"));
            ok("")
        });
        let msg = PublishMessage {
            topic: "pedidos".into(),
            key: Some("pedido-1".into()),
            value: "{\"id\":1}".into(),
            headers: vec![("trace".into(), "abc".into())],
            partition: None,
        };
        publish(&connection(), &msg).unwrap();
    }

    #[test]
    fn partition_targeted_publish_uses_kcat_or_explains_why_not() {
        let host = crate::mock_host();
        let msg = PublishMessage {
            topic: "t".into(),
            value: "v".into(),
            partition: Some(2),
            ..PublishMessage::default()
        };
        host.on_process(KCAT, |args, stdin| {
            assert!(args.contains(&"-p".to_owned()));
            assert!(args.contains(&"2".to_owned()));
            assert_eq!(stdin, Some("v"));
            ok("")
        });
        publish(&connection(), &msg).unwrap();
    }

    #[test]
    fn install_downloads_extracts_and_reports_the_scripts() {
        let host = crate::mock_host();
        // O mock não executa `tar` de verdade: registra o arquivo extraído.
        let dir = host::fs::plugin_dir().unwrap();
        host.add_external_file(
            &format!("{dir}/kafka_2.13-4.1.2/bin/kafka-topics.sh"),
            "#!/bin/sh\n",
        );
        host.on_process(CURL, |args, _| {
            assert!(args.iter().any(|a| a.ends_with(".tgz")));
            assert!(args.iter().any(|a| a.starts_with("https://")));
            ok("")
        });
        host.on_process(TAR, |args, _| {
            assert!(args.contains(&"-xzf".to_owned()));
            ok("")
        });
        let info = install("4.1.2", true).unwrap();
        assert!(info.bin.ends_with("/kafka_2.13-4.1.2/bin"));
        assert!(!installed_scripts().is_empty());
        assert_eq!(config::install_info(), Some(info));
    }

    #[test]
    fn authentication_errors_are_clear_and_redacted() {
        let _host = crate::mock_host();
        let out = ProcessOutput {
            status: Some(1),
            stdout: String::new(),
            stderr: "ERROR Authentication failed: sasl.password=hunter2".into(),
        };
        let e = classify(&out, TOPICS).unwrap().to_string();
        assert!(e.contains("Authentication failed"));
        assert!(!e.contains("hunter2"));
    }
}
