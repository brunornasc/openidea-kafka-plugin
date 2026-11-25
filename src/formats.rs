//! Payload formats offered by the Messages view.
//!
//! The names mirror the Kafka serializer classes. A topic stores bytes; the
//! (de)serializer is a client-side choice, so the selected format is applied
//! to `ConsoleConsumer` via `--formatter-property` and to `ConsoleProducer`
//! via `--command-property`. Kafka 4.x's console producer forces
//! `ByteArraySerializer` internally, so the text written to stdin is what
//! reaches the topic for every format.

use openidea_plugin::Error;

pub const STRING: &str = "StringSerializer";
pub const NAMES: [&str; 12] = [
    STRING,
    "ByteArraySerializer",
    "IntegerSerializer",
    "LongSerializer",
    "DoubleSerializer",
    "FloatSerializer",
    "BytesSerializer",
    "ByteBufferSerializer",
    "VoidSerializer",
    "JsonSerializer",
    "KafkaAvroSerializer",
    "KafkaProtobufSerializer",
];

/// Accepts the combobox values and a few short aliases. Empty/unknown values
/// fall back to `StringSerializer`.
pub fn normalize(value: &str) -> String {
    let value = value.trim();
    if let Some(found) = NAMES.iter().find(|name| name.eq_ignore_ascii_case(value)) {
        return (*found).into();
    }
    // O seletor de consumo mostra `ByteArrayDeserializer`, mas o valor guardado
    // é sempre o token `ByteArraySerializer`: a classe de deserialização é
    // derivada dele. Aceitar os dois evita confundir a pessoa e o storage.
    if let Some(short) = value.strip_suffix("Deserializer") {
        let canonical = format!("{short}Serializer");
        if let Some(found) = NAMES
            .iter()
            .find(|name| name.eq_ignore_ascii_case(&canonical))
        {
            return (*found).into();
        }
    }
    let short = value.rsplit('.').next().unwrap_or(value);
    if let Some(found) = NAMES.iter().find(|name| name.eq_ignore_ascii_case(short)) {
        return (*found).into();
    }
    let alias = match short.eq_ignore_ascii_case("String") {
        true => Some(STRING),
        false if short.eq_ignore_ascii_case("Avro") => Some("KafkaAvroSerializer"),
        false if short.eq_ignore_ascii_case("Protobuf") => Some("KafkaProtobufSerializer"),
        false if short.eq_ignore_ascii_case("Json") => Some("JsonSerializer"),
        false if short.eq_ignore_ascii_case("ByteArray") => Some("ByteArraySerializer"),
        false if short.eq_ignore_ascii_case("Integer") => Some("IntegerSerializer"),
        false if short.eq_ignore_ascii_case("Long") => Some("LongSerializer"),
        false if short.eq_ignore_ascii_case("Double") => Some("DoubleSerializer"),
        false if short.eq_ignore_ascii_case("Float") => Some("FloatSerializer"),
        false if short.eq_ignore_ascii_case("Bytes") => Some("BytesSerializer"),
        false if short.eq_ignore_ascii_case("ByteBuffer") => Some("ByteBufferSerializer"),
        false if short.eq_ignore_ascii_case("Void") => Some("VoidSerializer"),
        _ => None,
    };
    alias.unwrap_or(STRING).into()
}

fn class(format: &str, suffix: &str) -> &'static str {
    let format = normalize(format);
    let short = format.strip_suffix("Serializer").unwrap_or(&format);
    match short {
        "Json" => match suffix {
            "Serializer" => "org.apache.kafka.connect.json.JsonSerializer",
            _ => "org.apache.kafka.connect.json.JsonDeserializer",
        },
        "KafkaAvro" => match suffix {
            "Serializer" => "io.confluent.kafka.serializers.KafkaAvroSerializer",
            _ => "io.confluent.kafka.serializers.KafkaAvroDeserializer",
        },
        "KafkaProtobuf" => match suffix {
            "Serializer" => "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer",
            _ => "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer",
        },
        _ => match (short, suffix) {
            ("String", "Serializer") => "org.apache.kafka.common.serialization.StringSerializer",
            ("String", _) => "org.apache.kafka.common.serialization.StringDeserializer",
            ("ByteArray", "Serializer") => {
                "org.apache.kafka.common.serialization.ByteArraySerializer"
            }
            ("ByteArray", _) => "org.apache.kafka.common.serialization.ByteArrayDeserializer",
            ("Integer", "Serializer") => "org.apache.kafka.common.serialization.IntegerSerializer",
            ("Integer", _) => "org.apache.kafka.common.serialization.IntegerDeserializer",
            ("Long", "Serializer") => "org.apache.kafka.common.serialization.LongSerializer",
            ("Long", _) => "org.apache.kafka.common.serialization.LongDeserializer",
            ("Double", "Serializer") => "org.apache.kafka.common.serialization.DoubleSerializer",
            ("Double", _) => "org.apache.kafka.common.serialization.DoubleDeserializer",
            ("Float", "Serializer") => "org.apache.kafka.common.serialization.FloatSerializer",
            ("Float", _) => "org.apache.kafka.common.serialization.FloatDeserializer",
            ("Bytes", "Serializer") => "org.apache.kafka.common.serialization.BytesSerializer",
            ("Bytes", _) => "org.apache.kafka.common.serialization.BytesDeserializer",
            ("ByteBuffer", "Serializer") => {
                "org.apache.kafka.common.serialization.ByteBufferSerializer"
            }
            ("ByteBuffer", _) => "org.apache.kafka.common.serialization.ByteBufferDeserializer",
            ("Void", "Serializer") => "org.apache.kafka.common.serialization.VoidSerializer",
            _ => "org.apache.kafka.common.serialization.VoidDeserializer",
        },
    }
}

pub fn serializer_class(format: &str) -> &'static str {
    class(format, "Serializer")
}

pub fn deserializer_class(format: &str) -> &'static str {
    class(format, "Deserializer")
}

/// Nome amigável exibido no seletor/coluna de deserialização. O valor salvo
/// continua sendo o token `…Serializer`, mas a pessoa vê `…Deserializer`.
pub fn deserializer_name(format: &str) -> String {
    let format = normalize(format);
    format
        .strip_suffix("Serializer")
        .map(|short| format!("{short}Deserializer"))
        .unwrap_or(format)
}

/// Avro and Protobuf need Confluent jars and a schema registry. Make that
/// explicit instead of letting the JVM fail with a bare ClassNotFoundException.
pub fn requires_schema_registry(format: &str) -> bool {
    let format = normalize(format);
    format == "KafkaAvroSerializer" || format == "KafkaProtobufSerializer"
}

/// The publish form sends text. Numeric/String/Json formats are already the
/// wire representation; binary and registry-backed formats cannot be produced
/// from a text field without a custom serializer.
pub fn serialize_text(format: &str, text: &str) -> Result<String, Error> {
    let format = normalize(format);
    if requires_schema_registry(&format) {
        return Err(crate::i18n::tf(
            "{format} needs the Confluent serializer jar and a schema registry; the console producer only accepts text. Put the jar in extraClasspath and publish from a schema-aware client.",
            &[("format", &format)],
        )
        .into());
    }
    if format == "VoidSerializer" {
        return Err(crate::i18n::t(
            "VoidSerializer produces a null value; choose StringSerializer and leave the message value empty instead.",
        )
        .into());
    }
    Ok(text.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializer_names_normalize_to_the_saved_token() {
        let _host = crate::mock_host();
        assert_eq!(normalize("ByteArrayDeserializer"), "ByteArraySerializer");
        assert_eq!(
            deserializer_name("ByteArraySerializer"),
            "ByteArrayDeserializer"
        );
    }

    #[test]
    fn every_offered_format_has_a_class_pair() {
        let _host = crate::mock_host();
        for name in NAMES {
            let serializer = serializer_class(name);
            let deserializer = deserializer_class(name);
            assert!(serializer.ends_with("Serializer"), "{name}: {serializer}");
            assert!(
                deserializer.ends_with("Deserializer"),
                "{name}: {deserializer}"
            );
            assert_eq!(
                serializer.replace("Serializer", "Deserializer"),
                deserializer,
                "{name}"
            );
        }
        assert_eq!(
            serializer_class("StringSerializer"),
            "org.apache.kafka.common.serialization.StringSerializer"
        );
        assert_eq!(
            deserializer_class("JsonSerializer"),
            "org.apache.kafka.connect.json.JsonDeserializer"
        );
        assert_eq!(
            serializer_class("KafkaProtobufSerializer"),
            "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer"
        );
    }

    #[test]
    fn aliases_and_unknown_values_normalize() {
        assert_eq!(normalize(""), STRING);
        assert_eq!(normalize("integer"), "IntegerSerializer");
        assert_eq!(normalize("Avro"), "KafkaAvroSerializer");
        assert_eq!(
            normalize("org.apache.kafka.common.serialization.LongSerializer"),
            "LongSerializer"
        );
        assert_eq!(normalize("nonsense"), STRING);
    }

    #[test]
    fn schema_formats_fail_clearly_on_publish() {
        let _host = crate::mock_host();
        assert!(serialize_text("KafkaAvroSerializer", "{}").is_err());
        assert!(serialize_text("VoidSerializer", "x").is_err());
        assert_eq!(serialize_text("IntegerSerializer", "42").unwrap(), "42");
        assert_eq!(serialize_text("ByteArraySerializer", "hi").unwrap(), "hi");
    }
}
