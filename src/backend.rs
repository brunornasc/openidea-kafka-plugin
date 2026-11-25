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
use crate::formats;
use crate::i18n;
use crate::model::{
    self, ClusterInfo, FIELD_SEP, GroupLag, HEADER_KV, HEADER_SEP, MessageRecord, NULL_MARKER,
    QuorumInfo, RECORD_SEP, Topic,
};
use openidea_plugin::details::ProcessLog;
use openidea_plugin::{Error, HttpRequest, LogLevel, ProcessOutput, StatusItem, host};

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

/// Item da barra de status que sobrevive ao fim da chamada: o `Task` some,
/// mas este item mostra o progresso entre os ticks agendados.
const DOWNLOAD_STATUS: &str = "kafka-download";

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
    let result = host::run_process(program, args, stdin);
    // Todo processo é registrado no log, inclusive as tentativas de
    // compatibilidade (`java` primeiro, script depois). O curl é omitido: os
    // trechos do download já aparecem no progresso da view Install.
    if program != CURL {
        match &result {
            Ok(out) => {
                let status = match out.status {
                    Some(0) => "OK (exit 0)".into(),
                    Some(code) => format!("ERROR (exit {code})"),
                    None => "OK".into(),
                };
                record_process(
                    program,
                    args,
                    &status,
                    &format!("{}\n{}", out.stdout, out.stderr),
                );
            }
            Err(e) => record_process(program, args, &e.to_string(), ""),
        }
    }
    result
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

fn record_process(program: &str, args: &[String], status: &str, response: &str) {
    let mut command = program.to_owned();
    if !args.is_empty() {
        command.push(' ');
        command.push_str(&clip(&args.join(" "), 240));
    }
    config::append_log(config::CommandLogEntry {
        command,
        status: clip(status, 300),
        response: clip(&config::redact(response), 800),
    });
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
    let mut classpath = format!("{}/libs/*", info.dir.trim_end_matches('/'));
    let extra = config::extra_classpath();
    if !extra.is_empty() {
        classpath.push(':');
        classpath.push_str(&extra);
    }
    let mut out = vec![
        "-cp".into(),
        classpath,
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
    execute_with_trailing(c, tool, specific, |_| Ok(Vec::new()), stdin)
}

/// Como `execute`, mas com propriedades acrescentadas depois das globais: a
/// escolha feita na página de mensagens prevalece sobre as settings.
fn execute_with_trailing(
    c: &Connection,
    tool: Tool,
    specific: impl Fn(bool) -> Result<Vec<String>, Error>,
    trailing: impl Fn(bool) -> Result<Vec<String>, Error>,
    stdin: Option<&str>,
) -> Result<ProcessOutput, Error> {
    let modern = build_with_trailing(c, tool, true, specific(true)?, trailing(true)?)?;
    let legacy = build_with_trailing(c, tool, false, specific(false)?, trailing(false)?)?;
    // Um task mantém a chamada viva por até 60 s: a JVM + SASL costumam
    // demorar mais que o orçamento padrão de 5 s do host.
    let task = host::begin_task(
        &i18n::tf("Kafka: {program}", &[("program", tool.program())]),
        true,
    );
    let result = run_compat(tool, modern.clone(), legacy, stdin);
    if result.is_err() {
        // Sem segredos: o JAAS está em arquivo e `redact` esconde valores.
        host::log(
            LogLevel::Info,
            &format!(
                "Kafka CLI failed: {} {}",
                tool.program(),
                config::redact(&modern.join(" "))
            ),
        );
    }
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
    build_with_trailing(c, tool, modern, specific, Vec::new())
}

fn build_with_trailing(
    c: &Connection,
    tool: Tool,
    modern: bool,
    specific: Vec<String>,
    trailing: Vec<String>,
) -> Result<Vec<String>, Error> {
    let mut args = strings(&["--bootstrap-server", &c.bootstrap_servers]);
    // O arquivo de configuração é propriedade de `client_props`: uma única
    // flag, com o caminho do arquivo gerado ou o arquivo informado pelo usuário.
    // Flags vindas de `specific` são descartadas para que a CLI nunca receba
    // `--command-config` duas vezes (o ConsoleConsumer 4.x rejeita isso).
    let mut specific = specific.into_iter();
    while let Some(arg) = specific.next() {
        if matches!(
            arg.as_str(),
            "--command-config" | "--consumer.config" | "--producer.config"
        ) {
            let _ = specific.next();
            continue;
        }
        args.push(arg);
    }
    args.extend(client_props(c, tool, modern)?);
    // Propriedades da escolha por tópico vêm por último: a CLI Kafka usa a
    // última ocorrência de uma chave repetida em `--*-property`.
    args.extend(trailing);

    // Algumas versões do host repetem argumentos ao repassar o comando. Flags
    // de valor único não podem aparecer duas vezes na CLI Kafka (joptsimple
    // lança `MultipleArgumentsForOptionException`), então mantemos a primeira
    // ocorrência e descartamos as seguintes. Flags repetíveis
    // (`--formatter-property`, `--command-property`, `--config`, …) ficam.
    const SINGLE: &[&str] = &[
        "--bootstrap-server",
        "--topic",
        "--command-config",
        "--consumer.config",
        "--producer.config",
        "--partition",
        "--offset",
        "--timeout-ms",
        "--max-messages",
    ];
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(args.len());
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if SINGLE.contains(&arg.as_str()) && !seen.insert(arg.clone()) {
            i += if i + 1 < args.len() { 2 } else { 1 };
            continue;
        }
        out.push(arg.clone());
        i += 1;
    }
    Ok(out)
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
    let mut lines: Vec<String> = c
        .java_properties()
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect();
    lines.push(format!(
        "sasl.jaas.config={}",
        one_line_jaas(&c.sasl_jaas_config)
    ));
    let body = format!("{}\n", lines.join("\n"));
    let hash = stable_hash(&format!("{}|{}", c.id, body));
    let path = format!("{dir}/kafka-client-{hash:016x}.properties");
    host::fs::write_file(&path, &body).map_err(|e| {
        Error::Failed(i18n::tf(
            "Could not write the inline JAAS properties file: {error}",
            &[("error", &e.to_string())],
        ))
    })?;
    Ok(Some(path))
}

/// Classes de serialização configuradas globalmente. Deserializadores vão
/// em `--formatter-property` (ver `formatter_deserializers`): o
/// `DefaultMessageFormatter` só lê as suas próprias propriedades, não as do
/// consumidor.
fn configured_serde(tool: Tool) -> Vec<(String, String)> {
    let pairs: Vec<(&str, String)> = match tool {
        Tool::Producer => vec![
            ("key.serializer", config::key_serializer()),
            ("value.serializer", config::value_serializer()),
        ],
        _ => Vec::new(),
    };
    pairs
        .into_iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(key, value)| (key.into(), value))
        .collect()
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
    // Apenas ConsoleConsumer/ConsoleProducer aceitam `--command-property`
    // (ou `--consumer/--producer-property` no Kafka 3.x). Os comandos de
    // administração só leem configurações via `--command-config`.
    let property_flag = match (tool, modern) {
        (Tool::Consumer, false) => "--consumer-property",
        (Tool::Producer, false) => "--producer-property",
        (Tool::Consumer | Tool::Producer, true) => "--command-property",
        _ => "",
    };
    if !property_flag.is_empty() {
        // Extra properties in the connection win over the global setting.
        let mut props = c.java_properties();
        for (key, value) in configured_serde(tool) {
            if !props.iter().any(|(k, _)| k == &key) {
                props.push((key, value));
            }
        }
        for (key, value) in props {
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
    /// Formato escolhido para aquele tópico na página de mensagens.
    pub deserializer_format: String,
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

/// Classes que o `DefaultMessageFormatter` usa para decodificar chave/valor.
/// Vão em `--formatter-property` (não em `--command-property`): o formatter
/// só lê as suas próprias propriedades. Configurações globais servem como
/// fallback quando o formato escolhido é o padrão `String`.
fn formatter_deserializers(format: &str) -> [String; 2] {
    let format = formats::normalize(format);
    let default = formats::deserializer_class(&format);
    let custom = format == formats::STRING;
    let key = if custom && !config::key_deserializer().is_empty() {
        config::key_deserializer()
    } else {
        default.into()
    };
    let value = if custom && !config::value_deserializer().is_empty() {
        config::value_deserializer()
    } else {
        default.into()
    };
    [
        format!("key.deserializer={key}"),
        format!("value.deserializer={value}"),
    ]
}

/// Serializer classes chosen in the Messages view. Kafka 4.x overrides them
/// with ByteArraySerializer, but on 3.x they are honored and the values are
/// the standard wire representation of the form text. They are appended after
/// the global settings so this per-topic choice wins.
fn serializer_props(modern: bool, format: &str) -> Vec<String> {
    let flag = if modern {
        "--command-property"
    } else {
        "--producer-property"
    };
    let class = formats::serializer_class(format);
    strings(&[
        flag,
        &format!("key.serializer={class}"),
        flag,
        &format!("value.serializer={class}"),
    ])
}

/// Leitura estruturada: separadores de controle preservam valores com tabs,
/// vírgulas e quebras de linha.
fn formatter_props(modern: bool, format: &str) -> Vec<String> {
    let flag = if modern {
        "--formatter-property"
    } else {
        "--property"
    };
    let mut properties = vec![
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
    properties.extend(formatter_deserializers(format));
    formatter_args(flag, &properties)
}

/// Log ao vivo: formato legível para humanos (a aba de log não é parseada).
fn live_formatter_props(modern: bool, format: &str) -> Vec<String> {
    let flag = if modern {
        "--formatter-property"
    } else {
        "--property"
    };
    let mut properties = vec![
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
    properties.extend(formatter_deserializers(format));
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
        live_formatter_props(modern, &opts.deserializer_format)
    } else {
        formatter_props(modern, &opts.deserializer_format)
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
    // Formatos que não podem ser produzidos a partir de um campo de texto
    // (Avro/Protobuf/Void) falham aqui com uma explicação, não no JVM.
    let format = config::formats_for(&msg.topic).serializer;
    formats::serialize_text(&format, &msg.value)?;
    let msg = msg.clone();
    if msg.partition.is_some() {
        return publish_with_kcat(c, &msg);
    }
    execute_with_trailing(
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
        |modern| Ok(serializer_props(modern, &format)),
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

/// Infere a versão de `kafka_2.13-4.4.0.tgz`, para o comando de arquivo local
/// não precisar perguntar algo que já está no nome do arquivo.
pub fn version_from_tarball(path: &str) -> Option<String> {
    let name = path.trim().trim_end_matches('/').rsplit('/').next()?;
    let version = name
        .strip_prefix("kafka_2.13-")?
        .strip_suffix(".tgz")
        .or_else(|| name.strip_prefix("kafka_2.13-")?.strip_suffix(".tar.gz"))?;
    valid_version(version).then(|| version.to_owned())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DownloadStep {
    Complete,
    Incomplete,
}

fn remote_size(url: &str) -> Option<u64> {
    let out = raw(
        CURL,
        &strings(&["-sS", "-I", "-L", "--max-time", "5", url]),
        None,
    )
    .ok()?;
    if out.status != Some(0) {
        return None;
    }
    out.stdout.lines().rev().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        if key.eq_ignore_ascii_case("content-length") {
            value.trim().parse::<u64>().ok()
        } else {
            None
        }
    })
}

fn report_download(task: &host::Task, tarball: &str, total: Option<u64>) {
    let done = host::fs::stat(tarball)
        .map(|stat| stat.size)
        .unwrap_or_default();
    config::update_pending_progress(done, total);
    let message = config::download_progress(&config::PendingInstall {
        done,
        total,
        ..config::PendingInstall::default()
    });
    task.report(
        done.min(total.unwrap_or(u64::MAX)).min(u32::MAX as u64) as u32,
        total.map(|t| t.min(u32::MAX as u64) as u32),
        Some(&message),
    );
    // O Task termina com a chamada; este item fica visível até a conclusão.
    host::set_status_item(
        DOWNLOAD_STATUS,
        &StatusItem {
            text: format!("Kafka: {message}"),
            tooltip: Some(message),
            command: Some("install".into()),
        },
    );
}

fn clear_download() {
    config::clear_pending_install();
    host::remove_status_item(DOWNLOAD_STATUS);
}

/// Uma chamada do host dura no máximo 60 s. Em vez de um único curl longo,
/// fazemos quatro trechos de 10 s e reportamos o tamanho do arquivo após cada
/// um. `-C -` retoma o parcial; `Incomplete` não é erro, o agendador continua.
/// Se uma fonte falhar de imediato (404, DNS, capacidade não concedida),
/// tentamos a próxima sem desperdiçar os quatro trechos nela.
fn download_step(version: &str, tarball: &str, task: &host::Task) -> Result<DownloadStep, Error> {
    task.report(
        1,
        Some(3),
        Some(&i18n::t("Downloading the Apache Kafka tarball")),
    );
    let mut last_error: Option<String> = None;

    for url in config::download_urls(version) {
        let total = remote_size(&url);
        report_download(task, tarball, total);
        let mut try_next_source = false;

        for _ in 0..4 {
            let mut args = strings(&["-fL", "--connect-timeout", "20", "--max-time", "10"]);
            if host::fs::stat(tarball).is_ok() {
                args.extend(strings(&["-C", "-"]));
            }
            args.extend(strings(&["-o", tarball, &url]));
            let out = match raw(CURL, &args, None) {
                Ok(out) => out,
                Err(Error::Cancelled) => return Err(Error::Cancelled),
                Err(e) => {
                    last_error = Some(friendly(e).to_string());
                    try_next_source = true;
                    break;
                }
            };
            if task.is_cancelled() {
                return Err(Error::Cancelled);
            }
            report_download(task, tarball, total);
            match out.status {
                Some(0) => return Ok(DownloadStep::Complete),
                // 18: parcial; 28: `--max-time`; 56: rede caiu. Tentamos de novo
                // nesta mesma chamada, retomando o que já foi escrito.
                Some(18) | Some(28) | Some(56) => continue,
                status => {
                    let detail = config::redact(&format!("{}\n{}", out.stdout, out.stderr))
                        .lines()
                        .filter(|l| !l.trim().is_empty())
                        .take(4)
                        .collect::<Vec<_>>()
                        .join(" | ");
                    let code = match status {
                        Some(code) => code.to_string(),
                        None => "no exit code".into(),
                    };
                    last_error = Some(i18n::tf(
                        "Could not download {url}: curl exit {code}: {detail}. Check kafkaVersion and downloadUrl.",
                        &[
                            ("url", &url),
                            ("code", &code),
                            ("detail", &clip(&detail, 400)),
                        ],
                    ));
                    try_next_source = true;
                    break;
                }
            }
        }

        if !try_next_source {
            // Quatro trechos terminados por tempo ainda deixaram o arquivo
            // crescendo: a próxima chamada retoma de onde parou.
            return Ok(DownloadStep::Incomplete);
        }
    }

    Err(last_error
        .unwrap_or_else(|| i18n::t("Could not download the Kafka tarball."))
        .into())
}

fn verify_install(
    version: &str,
    root: &str,
    bin: &str,
    task: &host::Task,
) -> Result<config::InstallInfo, Error> {
    task.report(3, Some(3), Some(&i18n::t("Verifying the scripts")));
    let expected = format!("{bin}/{TOPICS}");
    host::fs::stat(&expected).map_err(|_| {
        i18n::tf(
            "The extraction finished, but {path} was not found. Check the tarball and the chosen version.",
            &[("path", &expected)],
        )
    })?;
    let info = config::InstallInfo {
        version: version.into(),
        dir: root.into(),
        bin: bin.into(),
    };
    config::set_install_info(&info)?;
    clear_download();
    Ok(info)
}

fn finish_install(
    version: &str,
    dir: &str,
    root: &str,
    bin: &str,
    tarball: &str,
    task: &host::Task,
) -> Result<config::InstallInfo, Error> {
    task.report(2, Some(3), Some(&i18n::t("Extracting the distribution")));
    run(TAR, &strings(&["-xzf", tarball, "-C", dir]), None)?;
    verify_install(version, root, bin, task)
}

/// Instala direto de um `.tgz` já salvo na máquina. Extraímos com `tar` a
/// partir do caminho escolhido; não é necessário copiar 130 MB para o
/// plugin-dir nem conceder `fs.read`, apenas a capacidade `process:tar`.
pub fn install_from_file(version: &str, source: &str) -> Result<config::InstallInfo, Error> {
    if !valid_version(version) {
        return Err(i18n::tf("Invalid version: {version}.", &[("version", version)]).into());
    }
    let source = source.trim();
    if source.is_empty() {
        return Err(i18n::t("Choose the local Kafka tarball.").into());
    }
    let dir = host::fs::plugin_dir()?;
    let root = format!("{}/kafka_2.13-{version}", dir.trim_end_matches('/'));
    let bin = format!("{root}/bin");
    let task = host::begin_task(&i18n::t("Kafka: installing from a local tarball"), true);
    task.report(1, Some(3), Some(&i18n::t("Extracting the distribution")));
    run(TAR, &strings(&["-xzf", source, "-C", &dir]), None)?;
    if task.is_cancelled() {
        return Err(Error::Cancelled);
    }
    verify_install(version, &root, &bin, &task)
}

/// Baixa e extrai a distribuição binária do Apache Kafka em `plugin-dir`.
/// `Ok(None)` significa que o download passou do limite de tempo e será
/// retomado automaticamente; nenhum erro é mostrado ao usuário nesse caso.
pub fn install(version: &str, force: bool) -> Result<Option<config::InstallInfo>, Error> {
    if !valid_version(version) {
        return Err(i18n::tf("Invalid version: {version}.", &[("version", version)]).into());
    }
    // Um segundo clique durante o download retoma o mesmo arquivo; dois curls
    // escrevendo no mesmo `.tgz` corromperiam o tarball.
    if config::pending_install().map_or(false, |pending| pending.version == version)
        && let Ok(finished) = resume_install()
    {
        return Ok(if finished {
            config::install_info()
        } else {
            None
        });
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
        clear_download();
        return Ok(Some(info));
    }
    let tarball = format!("{dir}/kafka_2.13-{version}.tgz");
    config::set_pending_install(version, &tarball)?;
    let task = host::begin_task(
        &i18n::tf("Kafka: installing {version}", &[("version", version)]),
        true,
    );
    match download_step(version, &tarball, &task) {
        Ok(DownloadStep::Complete) => {
            finish_install(version, &dir, &root, &bin, &tarball, &task).map(Some)
        }
        Ok(DownloadStep::Incomplete) => Ok(None),
        Err(e) => {
            clear_download();
            Err(e)
        }
    }
}

/// Continua um download pendente. Chamado a cada 60 s pelo evento agendado.
/// Retorna `true` quando a instalação terminou nesta chamada.
pub fn resume_install() -> Result<bool, Error> {
    let Some(pending) = config::pending_install() else {
        return Ok(false);
    };
    if !valid_version(&pending.version) {
        clear_download();
        return Ok(false);
    }
    let dir = host::fs::plugin_dir()?;
    let root = format!(
        "{}/kafka_2.13-{}",
        dir.trim_end_matches('/'),
        pending.version
    );
    let bin = format!("{root}/bin");
    if host::fs::stat(&format!("{bin}/{TOPICS}")).is_ok() {
        let info = config::InstallInfo {
            version: pending.version.clone(),
            dir: root,
            bin,
        };
        config::set_install_info(&info)?;
        clear_download();
        return Ok(true);
    }
    let task = host::begin_task(
        &i18n::tf(
            "Kafka: resuming {version}",
            &[("version", &pending.version)],
        ),
        false,
    );
    match download_step(&pending.version, &pending.tarball, &task) {
        Ok(DownloadStep::Complete) => {
            finish_install(&pending.version, &dir, &root, &bin, &pending.tarball, &task)?;
            Ok(true)
        }
        Ok(DownloadStep::Incomplete) => Ok(false),
        Err(e) => {
            clear_download();
            Err(e)
        }
    }
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
            assert!(!joined.contains("--command-property"));
            assert!(!joined.contains("security.protocol="));
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
        assert!(body.contains("request.timeout.ms=5000"), "{body}");
        assert!(body.contains("default.api.timeout.ms=5000"), "{body}");
        let jaas_line = body
            .lines()
            .find(|line| line.starts_with("sasl.jaas.config="))
            .unwrap();
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
        let text = config::log_text();
        assert!(text.contains("java "), "{text}");
        assert!(text.contains("> OK (exit 0)"), "{text}");
        assert!(text.contains("  t"), "{text}");
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
        host.on_process(CONSUMER, move |args, _| {
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
        let opts = ConsumeOptions {
            topic: "t".into(),
            ..ConsumeOptions::default()
        };
        consume(&c, &opts).unwrap();
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn admin_commands_do_not_send_unsupported_command_properties() {
        let host = crate::mock_host();
        let mut c = connection();
        c.security_protocol = "SASL_PLAINTEXT".into();
        c.sasl_mechanism = "PLAIN".into();
        host.on_process(TOPICS, |args, _| {
            assert!(!args.iter().any(|a| a == "--command-property"));
            assert!(!args.iter().any(|a| a.contains("security.protocol=")));
            ok("t\n")
        });
        list_topics(&c).unwrap();
    }

    #[test]
    fn configured_serde_classes_are_passed_only_to_console_tools() {
        let host = crate::mock_host();
        host.set_setting("keyDeserializer", r#""org.example.KeyDeserializer""#);
        host.set_setting("valueDeserializer", r#""org.example.ValueDeserializer""#);
        host.set_setting("keySerializer", r#""org.example.KeySerializer""#);
        host.set_setting("valueSerializer", r#""org.example.ValueSerializer""#);
        let c = connection();

        host.on_process(CONSUMER, |args, _| {
            assert!(args.iter().any(|a| a == "--formatter-property"));
            assert!(
                args.iter()
                    .any(|a| a == "key.deserializer=org.example.KeyDeserializer")
            );
            assert!(
                args.iter()
                    .any(|a| a == "value.deserializer=org.example.ValueDeserializer")
            );
            let joined = args.join("|");
            assert!(!joined.contains("--command-property|key.deserializer="));
            assert!(!joined.contains("--command-property|value.deserializer="));
            ok("")
        });
        host.on_process(PRODUCER, |args, _| {
            assert!(args.iter().any(|a| a == "--command-property"));
            assert!(
                args.iter()
                    .any(|a| a == "key.serializer=org.example.KeySerializer")
            );
            assert!(
                args.iter()
                    .any(|a| a == "value.serializer=org.example.ValueSerializer")
            );
            ok("")
        });
        host.on_process(TOPICS, |args, _| {
            assert!(!args.iter().any(|a| a.contains("deserializer=")));
            assert!(!args.iter().any(|a| a.contains("serializer=")));
            ok("t\n")
        });

        consume(
            &c,
            &ConsumeOptions {
                topic: "t".into(),
                ..ConsumeOptions::default()
            },
        )
        .unwrap();
        publish(
            &c,
            &PublishMessage {
                topic: "t".into(),
                value: "v".into(),
                ..PublishMessage::default()
            },
        )
        .unwrap();
        list_topics(&c).unwrap();
    }

    #[test]
    fn byte_array_serializer_is_accepted_as_wire_text() {
        let host = crate::mock_host();
        config::set_format("t", "serializer", "ByteArraySerializer").unwrap();
        host.on_process(PRODUCER, |args, stdin| {
            assert!(args.iter().any(|a| {
                a == "value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer"
            }));
            assert_eq!(stdin.unwrap(), "v\n");
            ok("")
        });
        publish(
            &connection(),
            &PublishMessage {
                topic: "t".into(),
                value: "v".into(),
                ..PublishMessage::default()
            },
        )
        .unwrap();
    }

    #[test]
    fn chosen_format_wins_over_global_serializer_and_uses_legacy_flag() {
        let host = crate::mock_host();
        host.set_setting("valueSerializer", r#""org.example.GlobalSerializer""#);
        config::set_format("t", "serializer", "LongSerializer").unwrap();
        host.on_process(PRODUCER, |args, _| {
            if args.iter().any(|a| a == "--command-property") {
                return Ok(ProcessOutput {
                    status: Some(1),
                    stdout: String::new(),
                    stderr: "unrecognized option: --command-property".into(),
                });
            }
            assert!(args.iter().any(|a| a == "--producer-property"));
            let serializers: Vec<_> = args
                .iter()
                .filter(|a| a.starts_with("value.serializer="))
                .collect();
            assert_eq!(
                serializers.last().unwrap().as_str(),
                "value.serializer=org.apache.kafka.common.serialization.LongSerializer"
            );
            ok("")
        });
        publish(
            &connection(),
            &PublishMessage {
                topic: "t".into(),
                value: "v".into(),
                ..PublishMessage::default()
            },
        )
        .unwrap();
    }

    #[test]
    fn build_sends_exactly_one_config_file_flag() {
        let _host = crate::mock_host();
        let mut c = connection();
        c.security_protocol = "SASL_PLAINTEXT".into();
        c.sasl_mechanism = "PLAIN".into();
        c.sasl_jaas_config =
            "org.example.LoginModule required username=\"dev\" password=\"secret\";".into();
        let specific = strings(&["--list", "--command-config", "/tmp/stale.properties"]);
        let args = build(&c, Tool::Topics, true, specific).unwrap();
        let flags = args
            .iter()
            .filter(|a| {
                matches!(
                    a.as_str(),
                    "--command-config" | "--consumer.config" | "--producer.config"
                )
            })
            .count();
        assert_eq!(flags, 1, "{args:?}");
        assert!(!args.iter().any(|a| a == "/tmp/stale.properties"));
        assert!(!args.iter().any(|a| a.contains("password=")));
    }

    #[test]
    fn build_collapses_duplicated_single_value_flags() {
        let _host = crate::mock_host();
        let c = connection();
        let specific = strings(&["--bootstrap-server", "other:9092", "--list"]);
        let args = build(&c, Tool::Topics, true, specific).unwrap();
        assert_eq!(
            args.iter()
                .filter(|a| a.as_str() == "--bootstrap-server")
                .count(),
            1
        );
        assert!(args.iter().any(|a| a.as_str() == "localhost:9092"));
        assert!(!args.iter().any(|a| a.as_str() == "other:9092"));
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
            if args.contains(&"-I".to_owned()) {
                return ok("");
            }
            assert!(args.iter().any(|a| a.ends_with(".tgz")));
            assert!(args.iter().any(|a| a.starts_with("https://")));
            // Quatro trechos curtos cabem no orçamento de 60 s e permitem
            // atualizar o progresso entre eles.
            assert!(args.iter().any(|a| a == "--max-time"));
            assert!(args.iter().any(|a| a == "10"));
            ok("")
        });
        host.on_process(TAR, |args, _| {
            assert!(args.contains(&"-xzf".to_owned()));
            ok("")
        });
        let info = install("4.1.2", true).unwrap().unwrap();
        assert!(info.bin.ends_with("/kafka_2.13-4.1.2/bin"));
        assert!(!installed_scripts().is_empty());
        assert_eq!(config::install_info(), Some(info));
        assert_eq!(config::pending_install(), None);
        let tasks = host.tasks();
        let task = tasks.last().unwrap();
        assert!(
            task.reports
                .iter()
                .any(|(_, _, msg)| msg.as_deref().map_or(false, |m| m.contains("Downloaded"))),
            "the task must report download progress"
        );
    }

    #[test]
    fn an_unavailable_cdn_falls_back_to_the_next_source() {
        let host = crate::mock_host();
        let dir = host::fs::plugin_dir().unwrap();
        host.add_external_file(
            &format!("{dir}/kafka_2.13-3.6.2/bin/kafka-topics.sh"),
            "#!/bin/sh\n",
        );
        host.on_process(CURL, |args, _| {
            if args.contains(&"-I".to_owned()) {
                return ok("");
            }
            if args.iter().any(|a| a.contains("dlcdn.apache.org")) {
                return Ok(ProcessOutput {
                    status: Some(22),
                    stdout: String::new(),
                    stderr: "HTTP 404".into(),
                });
            }
            ok("")
        });
        host.on_process(TAR, |_, _| ok(""));

        let info = install("3.6.2", true).unwrap().unwrap();
        assert_eq!(info.version, "3.6.2");
        let runs = host.process_runs();
        assert!(
            runs.iter()
                .any(|r| r.args.iter().any(|a| a.contains("dlcdn.apache.org")))
        );
        assert!(
            runs.iter()
                .any(|r| r.args.iter().any(|a| a.contains("downloads.apache.org")))
        );
    }

    #[test]
    fn a_timed_out_download_is_resumed_automatically() {
        let host = crate::mock_host();
        let dir = host::fs::plugin_dir().unwrap();
        let script = format!("{dir}/kafka_2.13-4.1.2/bin/kafka-topics.sh");
        host.add_external_file(&script, "#!/bin/sh\n");
        host.add_external_file(&format!("{dir}/kafka_2.13-4.1.2.tgz"), "partial");
        let calls = std::sync::atomic::AtomicUsize::new(0);
        host.on_process(CURL, move |args, _| {
            if args.contains(&"-I".to_owned()) {
                return ok("");
            }
            assert!(args.iter().any(|a| a == "--max-time"));
            assert!(
                args.windows(2).any(|w| w[0] == "-C" && w[1] == "-"),
                "every chunk must reuse the partial file"
            );
            if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 4 {
                Ok(ProcessOutput {
                    status: Some(28),
                    stdout: String::new(),
                    stderr: "Operation timed out after 10000 ms".into(),
                })
            } else {
                ok("")
            }
        });
        host.on_process(TAR, |_, _| ok(""));

        assert_eq!(install("4.1.2", true).unwrap(), None);
        let pending = config::pending_install().unwrap();
        assert_eq!(pending.version, "4.1.2");
        assert!(resume_install().unwrap());
        assert_eq!(config::pending_install(), None);
        assert_eq!(config::install_info().unwrap().version, "4.1.2");
    }

    #[test]
    fn the_version_is_inferred_from_a_local_tarball_name() {
        assert_eq!(
            version_from_tarball("/home/bruno/Downloads/kafka_2.13-4.4.0.tgz"),
            Some("4.4.0".into())
        );
        assert_eq!(version_from_tarball("/tmp/kafka.tgz"), None);
    }

    #[test]
    fn a_local_tarball_can_be_installed_without_downloading() {
        let host = crate::mock_host();
        let dir = host::fs::plugin_dir().unwrap();
        host.add_external_file(
            &format!("{dir}/kafka_2.13-4.1.2/bin/kafka-topics.sh"),
            "#!/bin/sh\n",
        );
        config::set_pending_install("4.1.2", "/tmp/old.tgz").unwrap();
        host.on_process(TAR, |args, _| {
            assert_eq!(args.first().map(String::as_str), Some("-xzf"));
            assert_eq!(args.get(1).map(String::as_str), Some("/tmp/kafka.tgz"));
            ok("")
        });
        let info = install_from_file("4.1.2", "/tmp/kafka.tgz").unwrap();
        assert_eq!(info.version, "4.1.2");
        assert_eq!(config::pending_install(), None);
    }

    #[test]
    fn chosen_formats_reach_both_console_tools() {
        let host = crate::mock_host();
        config::set_format("t", "serializer", "ByteArraySerializer").unwrap();
        config::set_format("t", "deserializer", "IntegerSerializer").unwrap();
        let c = connection();

        host.on_process(PRODUCER, |args, stdin| {
            assert!(args.iter().any(|a| {
                a == "value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer"
            }));
            assert!(stdin.unwrap().contains("hello"));
            ok("")
        });
        publish(
            &c,
            &PublishMessage {
                topic: "t".into(),
                value: "hello".into(),
                ..PublishMessage::default()
            },
        )
        .unwrap();

        host.on_process(CONSUMER, |args, _| {
            assert!(args.iter().any(|a| a == "--formatter-property"));
            assert!(
                args.iter()
                    .any(|a| a == "value.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer")
            );
            let record = format!(
                "CreateTime:1{FIELD_SEP}Partition:0{FIELD_SEP}Offset:0{FIELD_SEP}NO_HEADERS{FIELD_SEP}{NULL_MARKER}{FIELD_SEP}42{RECORD_SEP}"
            );
            ok(&record)
        });
        let messages = consume(
            &c,
            &ConsumeOptions {
                topic: "t".into(),
                deserializer_format: config::formats_for("t").deserializer,
                ..ConsumeOptions::default()
            },
        )
        .unwrap();
        assert_eq!(messages[0].value.as_deref(), Some("42"));
    }

    #[test]
    fn wire_text_is_preserved_for_display() {
        let host = crate::mock_host();
        host.on_process(CONSUMER, |_, _| {
            let record = format!(
                "CreateTime:1{FIELD_SEP}Partition:0{FIELD_SEP}Offset:0{FIELD_SEP}NO_HEADERS{FIELD_SEP}{NULL_MARKER}{FIELD_SEP}aGVsbG8={RECORD_SEP}"
            );
            ok(&record)
        });
        let messages = consume(
            &connection(),
            &ConsumeOptions {
                topic: "t".into(),
                deserializer_format: "StringSerializer".into(),
                ..ConsumeOptions::default()
            },
        )
        .unwrap();
        assert_eq!(messages[0].value.as_deref(), Some("aGVsbG8="));
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
