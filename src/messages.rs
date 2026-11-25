//! Buffer de mensagens consumidas e filtros do visualizador.

use crate::config::{self, ConsumePrefs};
use crate::model::MessageRecord;
use serde_json::json;

#[derive(Clone, Debug, Default)]
pub struct MessageBuffer {
    pub connection: String,
    pub topic: String,
    pub records: Vec<MessageRecord>,
    pub last_error: Option<String>,
    /// Quantas mensagens foram lidas desde a última limpeza.
    pub total_read: usize,
}

impl MessageBuffer {
    pub fn append(&mut self, connection: &str, topic: &str, records: Vec<MessageRecord>) {
        if self.connection != connection || self.topic != topic {
            self.records.clear();
        }
        self.connection = connection.into();
        self.topic = topic.into();
        self.total_read += records.len();
        self.records.extend(records);
        let keep = config::keep_messages();
        if self.records.len() > keep {
            self.records.drain(..self.records.len() - keep);
        }
    }

    pub fn clear(&mut self) {
        self.records.clear();
        self.total_read = 0;
        self.last_error = None;
    }

    /// Aplica os filtros de conteúdo, chave, header, offset e timestamp.
    pub fn filtered(&self, prefs: &ConsumePrefs) -> Vec<&MessageRecord> {
        let norm = |s: &str| {
            if prefs.case_sensitive {
                s.to_owned()
            } else {
                s.to_ascii_lowercase()
            }
        };
        let text = norm(&prefs.filter_text);
        let key = norm(&prefs.filter_key);
        let header = norm(&prefs.filter_header);
        let offset_from = prefs.offset_from.trim().parse::<i64>().ok();
        let offset_to = prefs.offset_to.trim().parse::<i64>().ok();
        let time_from = prefs.time_from.trim().parse::<i64>().ok();
        let time_to = prefs.time_to.trim().parse::<i64>().ok();
        self.records
            .iter()
            .filter(|m| {
                if !text.is_empty()
                    && !norm(m.value.as_deref().unwrap_or("")).contains(&text)
                {
                    return false;
                }
                if !key.is_empty() && !norm(m.key.as_deref().unwrap_or("")).contains(&key) {
                    return false;
                }
                if !header.is_empty() {
                    let haystack = norm(&m.headers_text());
                    if !haystack.contains(&header) {
                        return false;
                    }
                }
                if let Some(from) = offset_from
                    && m.offset < from
                {
                    return false;
                }
                if let Some(to) = offset_to
                    && m.offset > to
                {
                    return false;
                }
                if let Some(from) = time_from
                    && m.timestamp < from
                {
                    return false;
                }
                if let Some(to) = time_to
                    && m.timestamp > to
                {
                    return false;
                }
                true
            })
            .collect()
    }

    /// Estado completo enviado à página HTML do visualizador.
    pub fn to_json(&self, prefs: &ConsumePrefs, status: &str) -> String {
        let shown: Vec<&MessageRecord> = self.filtered(prefs);
        let messages: Vec<serde_json::Value> = shown
            .iter()
            .map(|m| {
                json!({
                    "topic": m.topic,
                    "partition": m.partition,
                    "offset": m.offset,
                    "timestamp": m.timestamp,
                    "timestampType": m.timestamp_type,
                    "key": m.key,
                    "value": m.value,
                    "headers": m.headers.iter().map(|(k, v)| json!({"key": k, "value": v})).collect::<Vec<_>>(),
                })
            })
            .collect();
        json!({
            "kind": "state",
            "prefs": serde_json::from_str::<serde_json::Value>(&prefs.to_json()).unwrap_or_default(),
            "status": status,
            "error": self.last_error,
            "connection": self.connection,
            "topic": self.topic,
            "total": self.total_read,
            "kept": self.records.len(),
            "shown": messages.len(),
            "messages": messages,
        })
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MessageRecord;
    

    fn buffer() -> MessageBuffer {
        let mut b = MessageBuffer::default();
        b.append(
            "local",
            "pedidos",
            vec![
                MessageRecord {
                    topic: "pedidos".into(),
                    partition: 0,
                    offset: 10,
                    timestamp: 1000,
                    key: Some("pedido-1".into()),
                    value: Some("{\"id\":1}".into()),
                    headers: vec![("trace".into(), Some("abc".into()))],
                    ..MessageRecord::default()
                },
                MessageRecord {
                    topic: "pedidos".into(),
                    partition: 1,
                    offset: 99,
                    timestamp: 9000,
                    key: Some("pedido-2".into()),
                    value: Some("OUTRO".into()),
                    ..MessageRecord::default()
                },
            ],
        );
        b
    }

    #[test]
    fn filters_by_content_key_header_offset_and_time() {
        let _host = crate::mock_host();
        let b = buffer();
        let mut p = ConsumePrefs::default();
        assert_eq!(b.filtered(&p).len(), 2);

        p.filter_text = "outro".into();
        assert_eq!(b.filtered(&p).len(), 1);
        p.filter_text.clear();
        p.filter_key = "pedido-1".into();
        assert_eq!(b.filtered(&p).len(), 1);
        p.filter_key.clear();
        p.filter_header = "trace".into();
        assert_eq!(b.filtered(&p).len(), 1);
        p.filter_header.clear();
        p.offset_from = "50".into();
        assert_eq!(b.filtered(&p).len(), 1);
        p.offset_from.clear();
        p.time_to = "5000".into();
        assert_eq!(b.filtered(&p).len(), 1);
    }

    #[test]
    fn case_insensitive_by_default_and_case_sensitive_when_asked() {
        let _host = crate::mock_host();
        let b = buffer();
        let mut p = ConsumePrefs { filter_text: "outro".into(), ..ConsumePrefs::default() };
        assert_eq!(b.filtered(&p).len(), 1);
        p.case_sensitive = true;
        assert_eq!(b.filtered(&p).len(), 0);
    }

    #[test]
    fn keeps_only_the_last_n_messages_and_serializes_state() {
        let _host = crate::mock_host();
        let b = buffer();
        let json = b.to_json(&ConsumePrefs::default(), "pronto");
        assert!(json.contains("\"shown\":2"));
        assert!(json.contains("pedido-1"));
        assert!(json.contains("\"kind\":\"state\""));
    }

    #[test]
    fn switching_topic_clears_the_buffer() {
        let _host = crate::mock_host();
        let mut b = buffer();
        b.append("local", "outro", vec![]);
        assert!(b.records.is_empty());
    }
}
