//! Modelos de domínio e parsers tolerantes da saída dos scripts do Kafka.
//!
//! As versões 3.x e 4.x mudam some[n]te alguns rótulos e colunas; os parsers
//! aceitam as duas formas e ignoram linhas de aviso (`Warning:`, logs SLF4J).

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Separadores de controle usados no `--property` do console consumer/producer.
/// Não aparecem em texto normal, então chaves/valores com tabs ou quebras de
/// linha continuam íntegros.
pub const FIELD_SEP: char = '\u{1f}';
pub const RECORD_SEP: char = '\u{1e}';
pub const HEADER_SEP: char = '\u{1d}';
pub const HEADER_KV: char = '\u{1c}';
pub const NULL_MARKER: &str = "\u{1b}NULL\u{1b}";

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topic {
    pub name: String,
    pub id: Option<String>,
    pub partitions: u32,
    pub replication_factor: u32,
    pub configs: Vec<(String, String)>,
    pub details: Vec<Partition>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Partition {
    pub id: i32,
    pub leader: Option<i32>,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub earliest: Option<i64>,
    pub latest: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Broker {
    pub id: i32,
    pub host: String,
    pub port: u16,
    pub rack: String,
    pub status: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumInfo {
    pub cluster_id: String,
    pub leader_id: Option<i32>,
    pub leader_epoch: Option<i64>,
    pub high_watermark: Option<i64>,
    pub current_voters: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub brokers: Vec<Broker>,
    pub quorum: Option<QuorumInfo>,
    pub controller_id: Option<i32>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupLag {
    pub group: String,
    pub topic: String,
    pub partition: i32,
    pub current: Option<i64>,
    pub log_end: i64,
    pub lag: i64,
    pub consumer_id: String,
    pub client_id: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub timestamp_type: String,
    pub key: Option<String>,
    pub value: Option<String>,
    pub headers: Vec<(String, Option<String>)>,
}

impl MessageRecord {
    pub fn headers_text(&self) -> String {
        if self.headers.is_empty() {
            return "-".into();
        }
        self.headers
            .iter()
            .map(|(k, v)| format!("{}={}", k, v.as_deref().unwrap_or("∅")))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Um valor `Campo: valor` de uma linha de `--describe`.
fn field(line: &str, key: &str) -> Option<String> {
    let needle = format!("{key}:");
    let start = line.find(&needle)? + needle.len();
    let rest = &line[start..];
    let value = rest
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim()
        .trim_end_matches([')', ',', ']']);
    (!value.is_empty()).then(|| value.to_owned())
}

fn ids(csv: &str) -> Vec<i32> {
    csv.split(',')
        .filter_map(|s| s.trim().parse::<i32>().ok())
        .collect()
}

fn is_noise(line: &str) -> bool {
    let l = line.trim();
    if l.is_empty() {
        return true;
    }
    let lower = l.to_ascii_lowercase();
    lower.starts_with("warning:")
        || lower.starts_with("warn ")
        || lower.starts_with("error:")
        || lower.starts_with("exception")
        || lower.starts_with("slf4j:")
        || lower.starts_with("[")
        || lower.contains("is deprecated")
        || lower.contains("use --")
        || l.starts_with('#')
}

pub fn parse_topic_list(stdout: &str) -> Vec<String> {
    let mut out: Vec<String> = stdout
        .lines()
        .map(str::trim)
        .filter(|l| !is_noise(l))
        .filter(|l| !l.contains(' '))
        .map(str::to_owned)
        .collect();
    out.sort();
    out.dedup();
    out
}

/// `kafka-topics.sh --describe`: uma linha de cabeçalho por tópico e uma linha
/// por partição. Aceita `Replicas`/`Isr`/`Elr` em qualquer ordem.
pub fn parse_topic_describe(stdout: &str) -> Vec<Topic> {
    let mut topics: BTreeMap<String, Topic> = BTreeMap::new();
    for raw in stdout.lines() {
        let line = raw.trim();
        if is_noise(line) || !line.contains("Topic:") {
            continue;
        }
        let name = match field(line, "Topic") {
            Some(n) => n,
            None => continue,
        };
        if let Some(partitions) = field(line, "PartitionCount") {
            let entry = topics.entry(name.clone()).or_insert_with(|| Topic {
                name: name.clone(),
                ..Topic::default()
            });
            entry.partitions = partitions.parse().unwrap_or(0);
            entry.replication_factor = field(line, "ReplicationFactor")
                .and_then(|v| v.parse().ok())
                .unwrap_or(entry.replication_factor);
            entry.id = field(line, "TopicId");
            if let Some(pos) = line.find("Configs:") {
                let rest = line[pos + "Configs:".len()..].trim();
                if !rest.is_empty() && rest != "null" {
                    entry.configs = rest
                        .split(',')
                        .filter_map(|p| p.split_once('='))
                        .map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()))
                        .collect();
                }
            }
            continue;
        }
        if let Some(id) = field(line, "Partition") {
            let entry = topics.entry(name.clone()).or_insert_with(|| Topic {
                name: name.clone(),
                ..Topic::default()
            });
            let partition = Partition {
                id: id.parse().unwrap_or(0),
                leader: field(line, "Leader").and_then(|v| v.parse().ok()),
                replicas: field(line, "Replicas").map(|v| ids(&v)).unwrap_or_default(),
                isr: field(line, "Isr")
                    .or_else(|| field(line, "ISR"))
                    .or_else(|| field(line, "Isrs"))
                    .map(|v| ids(&v))
                    .unwrap_or_default(),
                ..Partition::default()
            };
            if let Some(existing) = entry.details.iter_mut().find(|p| p.id == partition.id) {
                *existing = partition;
            } else {
                entry.details.push(partition);
            }
            entry.partitions = entry.partitions.max(entry.details.len() as u32);
        }
    }
    let mut out: Vec<Topic> = topics.into_values().collect();
    for t in &mut out {
        t.details.sort_by_key(|p| p.id);
        if t.replication_factor == 0 {
            t.replication_factor = t
                .details
                .first()
                .map(|p| p.replicas.len() as u32)
                .unwrap_or(0);
        }
    }
    out
}

pub fn parse_group_list(stdout: &str) -> Vec<String> {
    let mut out: Vec<String> = stdout
        .lines()
        .map(str::trim)
        .filter(|l| !is_noise(l))
        .filter(|l| !l.eq_ignore_ascii_case("GROUP"))
        .filter(|l| !l.contains(' '))
        .map(str::to_owned)
        .collect();
    out.sort();
    out.dedup();
    out
}

/// `kafka-consumer-groups.sh --describe --group <g>`: colunas separadas por
/// espaços. Offsets desconhecidos aparecem como `-`/`unknown`.
pub fn parse_group_describe(stdout: &str) -> Vec<GroupLag> {
    let mut rows = Vec::new();
    for raw in stdout.lines() {
        let line = raw.trim();
        if is_noise(line) || line.starts_with("GROUP") {
            continue;
        }
        let cols: Vec<&str> = line.split_whitespace().collect();
        if cols.len() < 6 {
            continue;
        }
        let Ok(partition) = cols[2].parse::<i32>() else {
            continue;
        };
        let Ok(log_end) = cols[4].parse::<i64>() else {
            continue;
        };
        let number = |s: &str| s.parse::<i64>().ok();
        rows.push(GroupLag {
            group: cols[0].into(),
            topic: cols[1].into(),
            partition,
            current: number(cols[3]),
            log_end,
            lag: number(cols[5]).unwrap_or(0),
            consumer_id: cols.get(6).unwrap_or(&"").to_string(),
            client_id: cols.get(8).unwrap_or(&"").to_string(),
        });
    }
    rows
}

/// `kafka-broker-api-versions.sh`: `host:port (id: 1 rack: null) -> (...)`.
pub fn parse_broker_versions(stdout: &str) -> Vec<Broker> {
    let mut brokers = Vec::new();
    for raw in stdout.lines() {
        let line = raw.trim();
        if is_noise(line) || !line.contains("id:") {
            continue;
        }
        let Some(id) = field(line, "id").and_then(|v| v.trim_end_matches(',').parse::<i32>().ok())
        else {
            continue;
        };
        let endpoint = line.split_whitespace().next().unwrap_or("");
        let (host, port) = endpoint.rsplit_once(':').unwrap_or((endpoint, "9092"));
        let port = port
            .trim_end_matches(|c: char| !c.is_ascii_digit())
            .parse::<u16>()
            .unwrap_or(9092);
        if brokers.iter().any(|b: &Broker| b.id == id) {
            continue;
        }
        brokers.push(Broker {
            id,
            host: host.trim_start_matches('[').trim_end_matches(']').into(),
            port,
            rack: field(line, "rack")
                .filter(|r| r != "null")
                .unwrap_or_default(),
            status: crate::i18n::t("available"),
        });
    }
    brokers.sort_by_key(|b| b.id);
    brokers
}

/// `kafka-metadata-quorum.sh describe --status` (KRaft).
pub fn parse_quorum(stdout: &str) -> Option<QuorumInfo> {
    let mut q = QuorumInfo::default();
    let mut found = false;
    for raw in stdout.lines() {
        let line = raw.trim();
        if let Some((key, value)) = line.split_once(':') {
            let key = key.trim().to_ascii_lowercase();
            let value = value.trim();
            found = true;
            match key.as_str() {
                "clusterid" => q.cluster_id = value.into(),
                "leaderid" => q.leader_id = value.parse().ok(),
                "leaderepoch" => q.leader_epoch = value.parse().ok(),
                "highwatermark" => q.high_watermark = value.parse().ok(),
                "currentvoters" => q.current_voters = value.into(),
                _ => {}
            }
        }
    }
    found.then_some(q)
}

/// `kafka-get-offsets.sh`: `topico:particao:offset`. Também entende o formato
/// do `kcat -Q` (`topico [particao] offset: N`).
pub fn parse_offsets(stdout: &str) -> Vec<(String, i32, i64)> {
    let mut out = Vec::new();
    for raw in stdout.lines() {
        let line = raw.trim();
        if is_noise(line) {
            continue;
        }
        if let Some(bracket) = line.find('[')
            && let Some(end) = line[bracket..].find(']')
        {
            let topic = line[..bracket].trim();
            let partition = line[bracket + 1..bracket + end].trim().parse::<i32>();
            let offset = line
                .rsplit("offset:")
                .next()
                .and_then(|s| s.trim().parse::<i64>().ok());
            if let (Ok(partition), Some(offset)) = (partition, offset) {
                out.push((topic.into(), partition, offset));
                continue;
            }
        }
        let parts: Vec<&str> = line.split(':').collect();
        if parts.len() >= 3
            && let (Ok(partition), Ok(offset)) = (
                parts[parts.len() - 2].trim().parse::<i32>(),
                parts[parts.len() - 1].trim().parse::<i64>(),
            )
        {
            out.push((parts[..parts.len() - 2].join(":"), partition, offset));
        }
    }
    out
}

fn unnull(value: &str) -> Option<String> {
    (value != NULL_MARKER && value != "null").then(|| value.to_owned())
}

fn parse_headers(text: &str) -> Vec<(String, Option<String>)> {
    if text.is_empty() || text == "NO_HEADERS" || text == NULL_MARKER {
        return Vec::new();
    }
    // O DefaultMessageFormatter escreve `chave:valor` e junta os headers com
    // `headers.separator` (GS). O separador FS é aceito para versões/formatos
    // que o utilizem.
    text.split(HEADER_SEP)
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair
                .split_once(HEADER_KV)
                .or_else(|| pair.split_once(':'))
                .unwrap_or((pair, ""));
            (k.to_owned(), unnull(v))
        })
        .collect()
}

/// Converte a saída do `DefaultMessageFormatter` com separadores de controle.
/// Ignora avisos de depreciação impressos no stdout antes do primeiro registro.
pub fn parse_messages(stdout: &str, topic: &str) -> Vec<MessageRecord> {
    const LABELS: [&str; 4] = [
        "CreateTime:",
        "LogAppendTime:",
        "NO_TIMESTAMP",
        "Partition:",
    ];
    let mut out = Vec::new();
    for chunk in stdout.split(RECORD_SEP) {
        let chunk = chunk.trim_matches('\n').trim_matches('\r');
        if chunk.is_empty() {
            continue;
        }
        // Remove ruído (linhas de aviso) que precede o primeiro registro.
        let start = LABELS
            .iter()
            .filter_map(|l| chunk.find(l))
            .min()
            .unwrap_or(0);
        let record = &chunk[start..];
        let separator = if record.contains(FIELD_SEP) {
            FIELD_SEP
        } else {
            '\t'
        };
        let parts: Vec<&str> = record.split(separator).collect();
        if parts.len() < 6 {
            continue;
        }
        let (timestamp_type, timestamp) = if let Some(ts) = parts[0].strip_prefix("CreateTime:") {
            ("CreateTime".into(), ts.parse().unwrap_or(0))
        } else if let Some(ts) = parts[0].strip_prefix("LogAppendTime:") {
            ("LogAppendTime".into(), ts.parse().unwrap_or(0))
        } else {
            (parts[0].into(), 0)
        };
        let partition = parts[1]
            .strip_prefix("Partition:")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let offset = parts[2]
            .strip_prefix("Offset:")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        out.push(MessageRecord {
            topic: topic.into(),
            partition,
            offset,
            timestamp,
            timestamp_type,
            key: unnull(parts[4]),
            value: unnull(&parts[5..].join(&separator.to_string())),
            headers: parse_headers(parts[3]),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_topic_describe_with_configs_and_partitions() {
        let out = concat!(
            "Topic: pedidos\tTopicId: abc123\tPartitionCount: 2\tReplicationFactor: 3\tConfigs: cleanup.policy=compact,min.insync.replicas=2\n",
            "\tTopic: pedidos\tPartition: 0\tLeader: 1\tReplicas: 1,2,3\tIsr: 1,2\tElr: \tLastKnownElr: \n",
            "\tTopic: pedidos\tPartition: 1\tLeader: 2\tReplicas: 2,3,1\tIsr: 2,3\tElr: \tLastKnownElr: \n",
            "Topic: __consumer_offsets\tTopicId: zzz\tPartitionCount: 50\tReplicationFactor: 1\tConfigs: \n",
        );
        let topics = parse_topic_describe(out);
        assert_eq!(topics.len(), 2);
        let t = &topics[0];
        assert_eq!(t.name, "__consumer_offsets");
        let t = &topics[1];
        assert_eq!(t.name, "pedidos");
        assert_eq!(t.partitions, 2);
        assert_eq!(t.replication_factor, 3);
        assert_eq!(t.configs.len(), 2);
        assert_eq!(t.details.len(), 2);
        assert_eq!(t.details[0].leader, Some(1));
        assert_eq!(t.details[0].replicas, vec![1, 2, 3]);
        assert_eq!(t.details[0].isr, vec![1, 2]);
    }

    #[test]
    fn parses_group_lag_with_unknown_offsets() {
        let out = concat!(
            "Warning: --zookeeper is deprecated\n",
            "GROUP           TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG   CONSUMER-ID      HOST      CLIENT-ID\n",
            "g1              pedidos 0          120             125             5     consumer-g1-1    /10.0.0.1  consumer-g1-1\n",
            "g1              pedidos 1          -              40              -     -               -          -\n",
        );
        let rows = parse_group_describe(out);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].current, Some(120));
        assert_eq!(rows[0].lag, 5);
        assert_eq!(rows[1].current, None);
        assert_eq!(rows[1].log_end, 40);
    }

    #[test]
    fn parses_brokers_and_quorum() {
        let _host = crate::mock_host();
        let brokers = parse_broker_versions(concat!(
            "localhost:9092 (id: 0 rack: null) -> (\n",
            "  Produce(0):\n",
            "b2:19092 (id: 2 rack: r1) -> (\n",
        ));
        assert_eq!(brokers.len(), 2);
        assert_eq!(brokers[0].id, 0);
        assert_eq!(brokers[0].host, "localhost");
        assert_eq!(brokers[0].port, 9092);
        assert_eq!(brokers[1].rack, "r1");

        let q = parse_quorum(concat!(
            "ClusterId:              MkU3OEVBNTcwNTJENDM2Qk\n",
            "LeaderId:               1\n",
            "LeaderEpoch:            14\n",
            "HighWatermark:          4242\n",
            "CurrentVoters:          [1,2,3]\n",
        ))
        .unwrap();
        assert_eq!(q.leader_id, Some(1));
        assert_eq!(q.leader_epoch, Some(14));
        assert_eq!(q.high_watermark, Some(4242));
    }

    #[test]
    fn parses_offsets_in_both_formats() {
        let out = concat!(
            "pedidos:0:10\n",
            "pedidos:1:99\n",
            "pedidos [2] offset: 42\n",
            "Warning: something\n",
        );
        assert_eq!(
            parse_offsets(out),
            vec![
                ("pedidos".into(), 0, 10),
                ("pedidos".into(), 1, 99),
                ("pedidos".into(), 2, 42),
            ]
        );
    }

    #[test]
    fn parses_console_consumer_records_with_control_separators() {
        let record = format!(
            "CreateTime:1690000000123{FIELD_SEP}Partition:3{FIELD_SEP}Offset:42{FIELD_SEP}trace:abc{HEADER_SEP}tenant:acme{FIELD_SEP}pedido-1{FIELD_SEP}{{\"id\":1}}{RECORD_SEP}"
        );
        let stdout = format!("Option --property is deprecated\n{record}");
        let messages = parse_messages(&stdout, "pedidos");
        assert_eq!(messages.len(), 1);
        let m = &messages[0];
        assert_eq!(m.topic, "pedidos");
        assert_eq!(m.partition, 3);
        assert_eq!(m.offset, 42);
        assert_eq!(m.timestamp, 1690000000123);
        assert_eq!(m.timestamp_type, "CreateTime");
        assert_eq!(m.key.as_deref(), Some("pedido-1"));
        assert_eq!(m.value.as_deref(), Some("{\"id\":1}"));
        assert_eq!(m.headers.len(), 2);
        assert_eq!(m.headers[0], ("trace".into(), Some("abc".into())));
        assert_eq!(m.headers_text(), "trace=abc, tenant=acme");
    }

    #[test]
    fn parses_null_key_and_no_headers() {
        let record = format!(
            "CreateTime:1{FIELD_SEP}Partition:0{FIELD_SEP}Offset:0{FIELD_SEP}NO_HEADERS{FIELD_SEP}{NULL_MARKER}{FIELD_SEP}hello{RECORD_SEP}"
        );
        let messages = parse_messages(&record, "t");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].key, None);
        assert!(messages[0].headers.is_empty());
        assert_eq!(messages[0].value.as_deref(), Some("hello"));
    }
}
