# Kafka — OpenIDEA plugin — Release 0.1.0

Artifact ready to publish: `dist/brunornasc.kafka-0.1.0.oiplugin`

- Built from `d03581c` (`plugin.toml` version stays `0.1.0`).
- SHA-256: `6596821b704baf4da6c2ad4e12b152df1e251dff00e963c69fa46443e04ba527`
- Size: 259.7 KiB. **Unsigned** package (no publisher key is present in this
  repository). For a signed marketplace package, generate/keep a key outside
  the repo and repack with
  `openidea-plugin pack . --key <key-file> --publisher brunornasc --out dist/brunornasc.kafka-0.1.0.oiplugin`.
- Validated with `cargo test` (75 tests), `openidea-plugin build`,
  `openidea-plugin check` and `openidea-plugin check` on the package.

## Highlights

- **Connections**: unlimited profiles, PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL,
  PLAIN/SCRAM, client properties file or inline JAAS for development (never in
  `argv` or logs), secrets redacted, and a real connection test before saving.
- **Cluster browsing**: topics, partitions, brokers, consumer groups and per
  group/topic/partition lag; sortable tables and a tree explorer with details.
- **Messages**: consume from beginning/end/offset/timestamp or live (native log
  tab), filter by text/key/header/offset/time, publish with key/headers and
  optional target partition via `kcat`. Beginning is the default start mode.
- **Message formats**: 12 serializer/deserializer choices with per-topic
  selection; deserializer labels in the UI, binary payloads shown as readable
  UTF-8 text and clear errors for formats that cannot come from a text field.
- **Local ack**: `✓`/`○` on each message plus a **Hide acked** filter. Acks are
  persisted locally and never commit an offset to the broker.
- **Command log**: last 80 entries in a `Command` / `Status` / `Response` table
  with readable `consume <topic>` / `publish <topic>` actions and redacted
  output; also available as an explorer tab.
- **CLI installer**: downloads Apache Kafka from the fast `dlcdn` CDN with
  fallbacks, persists progress, resumes interrupted transfers, supports a
  local tarball picker and runs the installed distribution directly with Java.
  Only `process:java` is required; the `kafka-*.sh` scripts and `kcat` are
  optional fallbacks.
- **CLI verification**: probes `kcat -V` (not `--version`, which kcat rejects),
  `java -version` and `--version` for the other tools, showing detected
  versions in the Kafka CLI view.
- **Language**: English by default, Brazilian Portuguese catalog shipped, and
  plugin-built text follows the IDE locale.

## Requirements

Java 17+ on the `PATH` for Kafka 4.x (11+ for 3.x). `kcat` is optional and only
needed to publish to a specific partition; it is not downloaded by the
installer. Approve the capabilities shown by the IDE when installing; missing
optional ones degrade gracefully.

## Install / publish

Users install the `.oiplugin` file through *Settings › Plugins › Install Plugin
from Disk…*. To publish it to a marketplace index, include the artifact in the
index (`openidea-plugin index <dir> …`) and use a signed package if the index
requires publisher verification.
