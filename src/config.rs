//! Perfis de conexão, preferências e estado persistente do plugin.
//!
//! O WASM não tem sockets TCP e o host não devolve o valor de um segredo:
//! portanto **o plugin nunca armazena senhas**. Credenciais SASL/SSL ficam num
//! arquivo `.properties` do usuário (fora do plugin), referenciado por caminho,
//! e são passadas aos scripts como `--command-config`. Propriedades extras com
//! nomes sensíveis são recusadas e qualquer saída de erro é redigida antes de
//! ser mostrada ou registrada.

use crate::i18n;
use openidea_plugin::{Error, host};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const PROFILES_KEY: &str = "kafka:profiles";
pub const ACTIVE_KEY: &str = "kafka:active";
pub const INSTALL_KEY: &str = "kafka:install";
pub const PREFS_KEY: &str = "kafka:prefs";

pub const SECURITY_PROTOCOLS: [&str; 4] = ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"];
pub const SASL_MECHANISMS: [&str; 4] = ["", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"];

/// Uma conexão com um cluster. Nenhum campo contém segredo.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Connection {
    pub id: String,
    pub name: String,
    /// `host:port`, separado por vírgulas.
    pub bootstrap_servers: String,
    pub security_protocol: String,
    pub sasl_mechanism: String,
    /// Usuário informado no formulário (usado pelo `kcat`; a senha fica no JAAS).
    pub sasl_username: String,
    /// Caminho absoluto de um arquivo `.properties` Java (contém
    /// `sasl.jaas.config`, truststores etc.). O plugin nunca lê o conteúdo.
    /// Opcional quando `sasl_jaas_config` é usado.
    pub client_properties: String,
    /// JAAS completo, para desenvolvimento. O plugin escreve um arquivo
    /// `.properties` no diretório privado e passa apenas o caminho à CLI.
    /// Fica salvo em texto simples junto com o perfil — uso dev, não produção.
    pub sasl_jaas_config: String,
    /// Caminho opcional de um arquivo de configuração `kcat` (`-F`).
    pub kcat_config: String,
    pub tls_ca: String,
    pub tls_cert: String,
    pub tls_key: String,
    pub tls_truststore: String,
    pub tls_keystore: String,
    /// Propriedades de cliente não sensíveis, no formato `chave=valor`.
    pub extra_properties: Vec<String>,
}

impl Connection {
    pub fn new(id: &str, name: &str) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            bootstrap_servers: String::new(),
            security_protocol: "PLAINTEXT".into(),
            ..Self::default()
        }
    }

    pub fn uses_sasl(&self) -> bool {
        self.security_protocol.starts_with("SASL_")
    }

    /// Valida o perfil. Senhas nunca aparecem aqui: o arquivo de propriedades
    /// é referenciado apenas pelo caminho.
    pub fn validate(&self) -> Result<(), Error> {
        if self.id.trim().is_empty() || self.name.trim().is_empty() {
            return Err(i18n::t("Give the connection a name.").into());
        }
        parse_bootstrap(&self.bootstrap_servers)?;
        if !SECURITY_PROTOCOLS.contains(&self.security_protocol.as_str()) {
            return Err(i18n::tf(
                "Invalid security protocol {protocol}. Use one of: {list}.",
                &[
                    ("protocol", self.security_protocol.as_str()),
                    ("list", &SECURITY_PROTOCOLS.join(", ")),
                ],
            )
            .into());
        }
        if !SASL_MECHANISMS.contains(&self.sasl_mechanism.as_str()) {
            return Err(i18n::tf(
                "Invalid SASL mechanism {mechanism}. Use PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512.",
                &[("mechanism", self.sasl_mechanism.as_str())],
            )
            .into());
        }
        if self.uses_sasl() && self.sasl_mechanism.is_empty() {
            return Err(i18n::t("Choose a SASL mechanism (PLAIN or SCRAM).").into());
        }
        if self.uses_sasl()
            && self.client_properties.trim().is_empty()
            && self.sasl_jaas_config.trim().is_empty()
        {
            return Err(i18n::t(
                "SASL connections need a client properties file or an inline sasl.jaas.config.",
            )
            .into());
        }
        for prop in &self.extra_properties {
            let (key, value) = split_property(prop)?;
            if is_secret_key(&key) {
                return Err(i18n::tf(
                    "Sensitive properties ({key}) are not accepted: put them in the client properties file.",
                    &[("key", key.as_str())],
                )
                .into());
            }
            if value.is_empty() {
                return Err(
                    i18n::tf("The property {key} has no value.", &[("key", key.as_str())]).into(),
                );
            }
        }
        Ok(())
    }

    /// Pares `chave=valor` das propriedades extras (já validadas).
    pub fn extra_pairs(&self) -> Vec<(String, String)> {
        self.extra_properties
            .iter()
            .filter_map(|p| split_property(p).ok())
            .collect()
    }

    pub fn apply_field(&mut self, key: &str, value: &str) {
        match key {
            "name" => self.name = value.into(),
            "bootstrap_servers" => self.bootstrap_servers = value.into(),
            "security_protocol" => self.security_protocol = value.into(),
            "sasl_mechanism" => self.sasl_mechanism = value.into(),
            "sasl_username" => self.sasl_username = value.into(),
            "client_properties" => self.client_properties = value.into(),
            "sasl_jaas_config" => self.sasl_jaas_config = value.into(),
            "kcat_config" => self.kcat_config = value.into(),
            "tls_ca" => self.tls_ca = value.into(),
            "tls_cert" => self.tls_cert = value.into(),
            "tls_key" => self.tls_key = value.into(),
            "tls_truststore" => self.tls_truststore = value.into(),
            "tls_keystore" => self.tls_keystore = value.into(),
            "extra_properties" => {
                self.extra_properties = value
                    .lines()
                    .map(str::trim)
                    .filter(|l| !l.is_empty())
                    .map(str::to_owned)
                    .collect();
            }
            _ => {}
        }
    }

    /// Propriedades Java derivadas dos caminhos TLS informados no formulário.
    /// Nenhuma contém segredo: apenas caminhos de arquivos.
    pub fn java_properties(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        if !self.tls_truststore.is_empty() {
            out.push((
                "ssl.truststore.location".into(),
                self.tls_truststore.clone(),
            ));
        }
        if !self.tls_keystore.is_empty() {
            out.push(("ssl.keystore.location".into(), self.tls_keystore.clone()));
        }
        if self.uses_sasl() {
            out.push(("security.protocol".into(), self.security_protocol.clone()));
            out.push(("sasl.mechanism".into(), self.sasl_mechanism.clone()));
        }
        // Falha rápido quando o broker não responde, em vez de deixar a JVM
        // presa até o limite do task do host.
        let timeout = timeout_ms().to_string();
        let extras = self.extra_pairs();
        let has = |key: &str| extras.iter().any(|(k, _)| k == key);
        if !has("request.timeout.ms") {
            out.push(("request.timeout.ms".into(), timeout.clone()));
        }
        if !has("default.api.timeout.ms") {
            out.push(("default.api.timeout.ms".into(), timeout));
        }
        out.extend(extras);
        out
    }

    /// Propriedades `librdkafka` para o `kcat` (`-X chave=valor`).
    /// Propriedades extras genéricas ficam no arquivo `kcat_config`: as chaves
    /// Java (`request.timeout.ms`, …) não existem no librdkafka.
    pub fn kcat_properties(&self) -> Vec<(String, String)> {
        let mut out = vec![("security.protocol".into(), self.security_protocol.clone())];
        if self.uses_sasl() {
            out.push(("sasl.mechanisms".into(), self.sasl_mechanism.clone()));
            if !self.sasl_username.is_empty() {
                out.push(("sasl.username".into(), self.sasl_username.clone()));
            }
        }
        if !self.tls_ca.is_empty() {
            out.push(("ssl.ca.location".into(), self.tls_ca.clone()));
        }
        if !self.tls_cert.is_empty() {
            out.push(("ssl.certificate.location".into(), self.tls_cert.clone()));
        }
        if !self.tls_key.is_empty() {
            out.push(("ssl.key.location".into(), self.tls_key.clone()));
        }
        out
    }

    /// Resumo das opções de segurança, sem segredos.
    pub fn security_summary(&self) -> String {
        let mut parts = vec![self.security_protocol.clone()];
        if self.uses_sasl() {
            parts.push(self.sasl_mechanism.clone());
            if !self.sasl_username.is_empty() {
                parts.push(format!("user={}", self.sasl_username));
            }
        }
        if !self.client_properties.is_empty() {
            parts.push(format!("config={}", self.client_properties));
        } else if !self.sasl_jaas_config.trim().is_empty() {
            parts.push(i18n::t("jaas=inline"));
        }
        parts.join(" · ")
    }
}

/// Verifica `host:port[,host:port]` e devolve os brokers normalizados.
pub fn parse_bootstrap(servers: &str) -> Result<Vec<String>, Error> {
    let trimmed = servers.trim();
    if trimmed.is_empty() {
        return Err(i18n::t("Enter at least one bootstrap server (host:port).").into());
    }
    let mut out = Vec::new();
    for part in trimmed.split(',') {
        let part = part.trim();
        let Some((host, port)) = part.rsplit_once(':') else {
            return Err(i18n::tf(
                "Invalid bootstrap server {part}: use host:port.",
                &[("part", part)],
            )
            .into());
        };
        let port: u32 = port
            .parse()
            .map_err(|_| i18n::tf("Invalid port in {part}.", &[("part", part)]))?;
        if host.is_empty() || port == 0 || port > 65535 {
            return Err(i18n::tf(
                "Invalid bootstrap server {part}: use host:port.",
                &[("part", part)],
            )
            .into());
        }
        if host.chars().any(char::is_control) || host.contains(' ') {
            return Err(i18n::tf("Invalid host in {part}.", &[("part", part)]).into());
        }
        out.push(format!("{host}:{port}"));
    }
    Ok(out)
}

fn split_property(prop: &str) -> Result<(String, String), Error> {
    let (key, value) = prop
        .split_once('=')
        .ok_or_else(|| i18n::tf("Invalid property {prop}: use key=value.", &[("prop", prop)]))?;
    let key = key.trim();
    if key.is_empty() {
        return Err(i18n::t("Property without a key (use key=value).").into());
    }
    Ok((key.to_owned(), value.trim().to_owned()))
}

/// Nomes que poderiam carregar um segredo: nunca entram em storage/argv/logs.
pub fn is_secret_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    [
        "password",
        "passwd",
        "secret",
        "token",
        "jaas",
        "credential",
        "private.key",
        "api.key",
        "apikey",
    ]
    .iter()
    .any(|s| k.contains(s))
}

/// Redige segredos que porventura apareçam na saída de um processo.
pub fn redact(text: &str) -> String {
    text.lines()
        .map(|line| {
            let lower = line.to_ascii_lowercase();
            if is_secret_key(&lower)
                || lower.contains("authorization:")
                || lower.contains("sasl.jaas.config")
            {
                // Mantém a chave, esconde o valor.
                if let Some((key, _)) = line.split_once('=') {
                    format!("{key}=<redacted>")
                } else if let Some((key, _)) = line.split_once(':') {
                    format!("{key}: <redacted>")
                } else {
                    "<redacted>".into()
                }
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------- storage

pub fn load_profiles() -> Vec<Connection> {
    let Ok(Some(raw)) = host::storage::get(PROFILES_KEY) else {
        return Vec::new();
    };
    serde_json::from_str(&raw).unwrap_or_default()
}

pub fn save_profiles(profiles: &[Connection]) -> Result<(), Error> {
    let raw = serde_json::to_string(profiles).map_err(|e| e.to_string())?;
    host::storage::set(PROFILES_KEY, &raw)
}

pub fn upsert(profile: Connection) -> Result<(), Error> {
    profile.validate()?;
    let mut profiles = load_profiles();
    if let Some(existing) = profiles.iter_mut().find(|p| p.id == profile.id) {
        *existing = profile;
    } else {
        profiles.push(profile);
    }
    save_profiles(&profiles)
}

pub fn remove(id: &str) -> Result<bool, Error> {
    let mut profiles = load_profiles();
    let before = profiles.len();
    profiles.retain(|p| p.id != id);
    if profiles.len() == before {
        return Ok(false);
    }
    save_profiles(&profiles)?;
    if active_id().as_deref() == Some(id) {
        let next = profiles.first().map(|p| p.id.clone()).unwrap_or_default();
        set_active(&next);
    }
    Ok(true)
}

pub fn by_id(id: &str) -> Option<Connection> {
    load_profiles().into_iter().find(|p| p.id == id)
}

pub fn active_id() -> Option<String> {
    host::storage::get(ACTIVE_KEY)
        .ok()
        .flatten()
        .filter(|s| !s.is_empty())
}

pub fn set_active(id: &str) {
    let _ = host::storage::set(ACTIVE_KEY, id);
}

pub fn active() -> Option<Connection> {
    let id = active_id()?;
    by_id(&id).or_else(|| load_profiles().into_iter().next())
}

/// Gera um id estável a partir do nome, sem expor dados sensíveis.
pub fn new_id(name: &str) -> String {
    let slug: String = name
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let slug = slug.trim_matches('-').to_owned();
    let base = if slug.is_empty() {
        "cluster".into()
    } else {
        slug
    };
    let profiles = load_profiles();
    if !profiles.iter().any(|p| p.id == base) {
        return base;
    }
    (2..)
        .map(|n| format!("{base}-{n}"))
        .find(|candidate| !profiles.iter().any(|p| &p.id == candidate))
        .unwrap_or(base)
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct InstallInfo {
    pub version: String,
    /// Diretório da distribuição extraída.
    pub dir: String,
    /// `bin/` da distribuição.
    pub bin: String,
}

pub fn install_info() -> Option<InstallInfo> {
    let raw = host::storage::get(INSTALL_KEY).ok().flatten()?;
    serde_json::from_str(&raw).ok()
}

pub fn set_install_info(info: &InstallInfo) -> Result<(), Error> {
    host::storage::set(
        INSTALL_KEY,
        &serde_json::to_string(info).map_err(|e| e.to_string())?,
    )
}

/// Preferências do visualizador de mensagens.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ConsumePrefs {
    pub topic: String,
    pub partition: String,
    /// `beginning`, `end`, `offset` ou `timestamp`.
    pub start: String,
    pub offset: String,
    pub limit: u32,
    pub filter_text: String,
    pub filter_key: String,
    pub filter_header: String,
    pub offset_from: String,
    pub offset_to: String,
    /// Timestamp em ms (época) para limitar o intervalo.
    pub time_from: String,
    pub time_to: String,
    pub case_sensitive: bool,
}

impl Default for ConsumePrefs {
    fn default() -> Self {
        Self {
            topic: String::new(),
            partition: String::new(),
            start: "end".into(),
            offset: String::new(),
            limit: 200,
            filter_text: String::new(),
            filter_key: String::new(),
            filter_header: String::new(),
            offset_from: String::new(),
            offset_to: String::new(),
            time_from: String::new(),
            time_to: String::new(),
            case_sensitive: false,
        }
    }
}

impl ConsumePrefs {
    pub fn load() -> Self {
        let Ok(Some(raw)) = host::storage::get(PREFS_KEY) else {
            return Self::default();
        };
        serde_json::from_str(&raw).unwrap_or_default()
    }

    pub fn save(&self) {
        if let Ok(raw) = serde_json::to_string(self) {
            let _ = host::storage::set(PREFS_KEY, &raw);
        }
    }

    pub fn to_json(&self) -> String {
        json!({
            "topic": self.topic,
            "partition": self.partition,
            "start": self.start,
            "offset": self.offset,
            "limit": self.limit,
            "filterText": self.filter_text,
            "filterKey": self.filter_key,
            "filterHeader": self.filter_header,
            "offsetFrom": self.offset_from,
            "offsetTo": self.offset_to,
            "timeFrom": self.time_from,
            "timeTo": self.time_to,
            "caseSensitive": self.case_sensitive,
        })
        .to_string()
    }

    /// Aplica um patch JSON vindo da página HTML.
    pub fn patch(&mut self, value: &serde_json::Value) {
        if let Some(v) = value.get("topic").and_then(|v| v.as_str()) {
            self.topic = v.into();
        }
        if let Some(v) = value.get("partition").and_then(|v| v.as_str()) {
            self.partition = v.into();
        }
        if let Some(v) = value.get("start").and_then(|v| v.as_str()) {
            self.start = v.into();
        }
        if let Some(v) = value.get("offset").and_then(|v| v.as_str()) {
            self.offset = v.into();
        }
        if let Some(v) = value.get("limit").and_then(|v| v.as_u64()) {
            self.limit = v.clamp(1, 5000) as u32;
        }
        if let Some(v) = value.get("filterText").and_then(|v| v.as_str()) {
            self.filter_text = v.into();
        }
        if let Some(v) = value.get("filterKey").and_then(|v| v.as_str()) {
            self.filter_key = v.into();
        }
        if let Some(v) = value.get("filterHeader").and_then(|v| v.as_str()) {
            self.filter_header = v.into();
        }
        if let Some(v) = value.get("offsetFrom").and_then(|v| v.as_str()) {
            self.offset_from = v.into();
        }
        if let Some(v) = value.get("offsetTo").and_then(|v| v.as_str()) {
            self.offset_to = v.into();
        }
        if let Some(v) = value.get("timeFrom").and_then(|v| v.as_str()) {
            self.time_from = v.into();
        }
        if let Some(v) = value.get("timeTo").and_then(|v| v.as_str()) {
            self.time_to = v.into();
        }
        if let Some(v) = value.get("caseSensitive").and_then(|v| v.as_bool()) {
            self.case_sensitive = v;
        }
    }
}

// ---------------------------------------------------------------- settings

pub fn setting_str(key: &str, default: &str) -> String {
    host::setting_string(key).unwrap_or_else(|| default.into())
}

pub fn setting_num(key: &str, default: f64) -> f64 {
    host::setting_number(key).unwrap_or(default)
}

pub fn setting_bool(key: &str, default: bool) -> bool {
    host::setting_bool(key).unwrap_or(default)
}

pub fn max_messages() -> u32 {
    setting_num("maxMessages", 200.0).clamp(1.0, 5000.0) as u32
}

pub fn timeout_ms() -> u32 {
    setting_num("timeoutMs", 5000.0).clamp(500.0, 120_000.0) as u32
}

pub fn keep_messages() -> usize {
    setting_num("keepMessages", 500.0).clamp(50.0, 5000.0) as usize
}

pub fn kafka_version() -> String {
    setting_str("kafkaVersion", "4.1.2")
}

pub fn download_url(version: &str) -> String {
    setting_str(
        "downloadUrl",
        "https://downloads.apache.org/kafka/{version}/kafka_2.13-{version}.tgz",
    )
    .replace("{version}", version)
}

pub fn cli_flavour() -> String {
    setting_str("cliFlavour", "auto")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_servers_are_validated() {
        let _host = crate::mock_host();
        assert_eq!(
            parse_bootstrap("localhost:9092").unwrap(),
            vec!["localhost:9092"]
        );
        assert_eq!(
            parse_bootstrap(" a:9092 , b:19092 ").unwrap(),
            vec!["a:9092", "b:19092"]
        );
        for bad in ["", "localhost", "localhost:0", "localhost:99999", "a:1,b"] {
            assert!(parse_bootstrap(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn profiles_never_store_secrets_and_sasl_requires_credentials() {
        let _host = crate::mock_host();
        let mut c = Connection::new("local", "Local");
        c.bootstrap_servers = "localhost:9092".into();
        c.validate().unwrap();

        c.security_protocol = "SASL_SSL".into();
        c.sasl_mechanism = "SCRAM-SHA-256".into();
        assert!(c.validate().is_err(), "SASL sem credenciais");
        c.client_properties = "/home/user/kafka.properties".into();
        c.validate().unwrap();
        c.client_properties.clear();
        c.sasl_jaas_config =
            "org.example.LoginModule required username=\"dev\" password=\"x\";".into();
        c.validate().unwrap();
        c.sasl_jaas_config.clear();

        c.extra_properties.push("sasl.password=segredo".into());
        assert!(c.validate().is_err(), "senha em propriedade extra");
        c.extra_properties.clear();
        c.extra_properties.push("request.timeout.ms=10000".into());
        c.client_properties = "/home/user/kafka.properties".into();
        c.validate().unwrap();

        upsert(c.clone()).unwrap();
        let stored = host::storage::get(PROFILES_KEY).unwrap().unwrap();
        assert!(!stored.contains("segredo"));
        assert_eq!(load_profiles(), vec![c]);
    }

    #[test]
    fn redaction_hides_values_but_keeps_keys() {
        let text = "sasl.password=abc123\nrequest.timeout.ms=1000\nAuthorization: Bearer xyz";
        let out = redact(text);
        assert!(out.contains("sasl.password=<redacted>"));
        assert!(out.contains("request.timeout.ms=1000"));
        assert!(!out.contains("abc123"));
        assert!(!out.contains("xyz"));
    }

    #[test]
    fn tls_and_sasl_settings_become_client_properties() {
        let _host = crate::mock_host();
        let mut c = Connection::new("secure", "Secure");
        c.bootstrap_servers = "broker:9093".into();
        c.security_protocol = "SASL_SSL".into();
        c.sasl_mechanism = "SCRAM-SHA-256".into();
        c.sasl_username = "dev".into();
        c.client_properties = "/etc/kafka/client.properties".into();
        c.tls_ca = "/etc/kafka/ca.pem".into();
        c.tls_truststore = "/etc/kafka/truststore.jks".into();
        c.tls_keystore = "/etc/kafka/keystore.jks".into();
        c.extra_properties = vec!["request.timeout.ms=10000".into()];
        c.validate().unwrap();

        let java = c.java_properties();
        assert!(java.contains(&(
            "ssl.truststore.location".into(),
            "/etc/kafka/truststore.jks".into()
        )));
        assert!(java.contains(&(
            "ssl.keystore.location".into(),
            "/etc/kafka/keystore.jks".into()
        )));
        assert!(java.contains(&("security.protocol".into(), "SASL_SSL".into())));
        assert!(java.contains(&("sasl.mechanism".into(), "SCRAM-SHA-256".into())));
        assert!(java.contains(&("request.timeout.ms".into(), "10000".into())));

        let kcat = c.kcat_properties();
        assert!(kcat.contains(&("security.protocol".into(), "SASL_SSL".into())));
        assert!(kcat.contains(&("sasl.mechanisms".into(), "SCRAM-SHA-256".into())));
        assert!(kcat.contains(&("sasl.username".into(), "dev".into())));
        assert!(kcat.contains(&("ssl.ca.location".into(), "/etc/kafka/ca.pem".into())));
        assert!(kcat.iter().all(|(k, _)| !is_secret_key(k)));
    }

    #[test]
    fn active_profile_falls_back_to_the_first_one() {
        let _host = crate::mock_host();
        let mut c = Connection::new("a", "A");
        c.bootstrap_servers = "localhost:9092".into();
        upsert(c.clone()).unwrap();
        set_active("missing");
        assert_eq!(active(), Some(c));
    }
}
