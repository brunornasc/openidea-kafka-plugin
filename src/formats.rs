//! Payload formats offered by the Messages view.
//!
//! The names mirror the Kafka serializer classes. A topic stores bytes; the
//! (de)serializer is a client-side choice, so the selected format is applied
//! to `ConsoleConsumer` via `--formatter-property` and to `ConsoleProducer`
//! via `--command-property`. Both Kafka 3.x and 4.x force
//! `ByteArraySerializer` internally in `ConsoleProducer`, so only formats
//! whose wire bytes are the UTF-8 text itself can be published; the others
//! are rejected before writing.

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

/// Formatos que o console producer consegue publicar a partir do campo de
/// texto: as bytes UTF-8 digitadas são exatamente a representação na wire.
pub const PUBLISHABLE: [&str; 3] = [STRING, "ByteArraySerializer", "JsonSerializer"];

pub fn publishable(format: &str) -> bool {
    let format = normalize(format);
    PUBLISHABLE.contains(&format.as_str())
}

/// The publish form sends UTF-8 text to `ConsoleProducer`, which always uses
/// `ByteArraySerializer` internally. Formats with a binary wire encoding
/// (numeric, Bytes/ByteBuffer and the registry-backed ones) cannot be produced
/// that way: passing a serializer class name does not change the reader. Fail
/// before writing instead of reporting a successful but unreadable record.
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
    if !publishable(&format) {
        return Err(crate::i18n::tf(
            "{format} cannot be published from a text field: ConsoleProducer writes the UTF-8 bytes you type, so a consumer using {format} would reject the record. Choose StringSerializer, ByteArraySerializer or JsonSerializer.",
            &[("format", &format)],
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
    }

    #[test]
    fn only_text_wire_formats_can_be_published_from_the_form() {
        let _host = crate::mock_host();
        for format in PUBLISHABLE {
            assert_eq!(serialize_text(format, "42").unwrap(), "42");
        }
        // Numeric and Bytes serializers have a binary wire encoding that the
        // console producer cannot emit from a text line.
        for format in [
            "IntegerSerializer",
            "LongSerializer",
            "DoubleSerializer",
            "FloatSerializer",
            "BytesSerializer",
            "ByteBufferSerializer",
        ] {
            let error = serialize_text(format, "42").unwrap_err().to_string();
            assert!(error.contains(format), "{error}");
            assert!(!publishable(format));
        }
    }
}
