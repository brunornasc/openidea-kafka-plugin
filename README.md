# Kafka — OpenIDEA plugin

An OpenIDEA plugin to **manage and inspect Apache Kafka clusters**: connection
profiles, topics, partitions, brokers, consumer groups, message consumption
(bounded, filtered and live) and publishing.

```sh
cargo test                       # logic, against the SDK's MockHost (75 tests)
openidea-plugin build            # → dist/plugin.wasm, validated like an install
openidea-plugin check            # validates plugin.toml, locales and the component
openidea-plugin link             # loads this folder in the IDE (Settings › Plugins)
```

`plugin.toml` declares 27 commands (they appear in Find Action before the code
is loaded), 7 tool windows and the capabilities the code needs.

> **Language:** the UI is **English (`en-US`) by default** and ships a complete
> Brazilian Portuguese translation. See [Language](#language) below.

---

## Why an external CLI

The plugin runs in the WASM sandbox: **there are no TCP sockets** in the
OpenIDEA SDK. Its I/O is HTTP(S)/WebSocket (`net`), Unix sockets (`ipc`) and
approved processes (`process:<program>`). Kafka speaks a binary protocol over
TCP, so the plugin **does not talk to the broker directly**: it runs the
official **Apache Kafka** distribution scripts, which are the external backend.
That is why the plugin can **download and install that CLI itself** — no manual
setup beyond a JRE is needed.

```
OpenIDEA (WASM) --host::run_process--> kafka-topics.sh / console-consumer.sh / …
                                      consumer-groups.sh / get-offsets.sh …
                                              |
                                              v  (Kafka protocol, TCP)
                                       the user's cluster
```

### Installing the backend

1. **Kafka: Download and Install the Apache Kafka CLI** picks a version from
   the catalog (the `https://downloads.apache.org/kafka/` index when the
   `net:downloads.apache.org` capability is granted, otherwise the built-in
   list 4.4.0 → 3.6.2), downloads `kafka_2.13-<version>.tgz` with `curl` and
   extracts it with `tar` into the plugin's private directory
   (`host::fs::plugin_dir()`). The download tries the fast `dlcdn.apache.org`
   CDN first, then `downloads.apache.org` and `archive.apache.org` for older
   releases; a configured `downloadUrl` replaces that list. A host call lasts
   at most 60 s and the tarball is
   ~130 MB: each call runs four `curl` chunks of 10 s that resume with `-C -`.
   The size and percentage (`Downloaded 42 MB of 129 MB (32%)`) are persisted,
   shown in the status bar and refreshed in the Install view on every tick, so
   it stays visible even after the call ends. When a call ends before the file
   is complete, the download continues automatically on the next scheduled
   tick.
 2. If you already have `kafka_2.13-<version>.tgz` on your machine (for example
   from a previous download), use **Kafka: Install CLI from a Local Tarball**
   (or the **Install from Local File** button in the Install view). A folder
   dialog opens, the `.tgz` files in it are listed for you to pick; grant the
   optional `fs.read:~` capability to enable the listing. Without that grant,
   the chosen folder is prefilled in a text field so you only type the filename.
   The version is inferred from `kafka_2.13-<version>.tgz` (otherwise the
   configured `kafkaVersion` is used). `~/` is not expanded in the typed path.
 3. The installed distribution is launched directly as
    `java -cp <plugin-dir>/kafka_2.13-<version>/libs/* <MainClass> …`, so no
    `process:*.sh` grant is needed and scripts approved before installation do
    not have to be re-approved. Only `process:java` is required; the host still
    resolves Java when the permission is approved, but Java is already on the
    `PATH`.
 4. The **Kafka CLI** view shows the installed version, the catalog and the
    state of each program; **Kafka: Verify CLI Installation** probes the
    grants. Each tool is asked for its version with the flag it accepts:
    `kcat -V` (`--version` is an unknown argument for kcat), `java -version`
    (also works on Java 8) and `--version` for curl, tar and the Kafka
    scripts. The detected versions appear next to each program, and Java's
    own version is shown separately. The `process:kafka-*.sh` capabilities
    remain as an optional fallback for a Kafka installed outside the plugin;
    **Kafka: Copy Script Paths** copies those paths.
 5. **Prerequisite:** Java 17+ on the `PATH` for Kafka 4.x (11+ for 3.x).
    `kcat` is optional and only needed to publish to a specific partition
    (install it with your OS package manager).

If a Kafka distribution is already installed outside the plugin, skip step 1
and point the optional script capabilities at its `bin/` scripts.

---

## Requirements covered

### 1. Connections / servers

- Unlimited profiles in `host::storage` (`kafka:profiles`): id, name,
  bootstrap servers (`host:port[,host:port]`, validated), security protocol,
  SASL mechanism, username, TLS paths, client properties file, inline JAAS
  config and extra properties. Multiple environments coexist; the active one
  is in `kafka:active`.
- Security: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` with `PLAIN`,
  `SCRAM-SHA-256` or `SCRAM-SHA-512`. The **Security protocol** and **SASL
  mechanism** chosen in the form go into the generated client properties file
  passed with `--command-config` (plus `-X` for `kcat`). TLS: CA/certificate/key
  (PEM, for `kcat`) and truststore/keystore (JKS, for the Java tools), also
  forwarded as client properties. Admin commands (`TopicCommand`,
  `ConsumerGroupCommand`, `GetOffsetShell`, `BrokerApiVersionsCommand`,
  `MetadataQuorumCommand`) do **not** accept `--command-property`; only
  `ConsoleConsumer`/`ConsoleProducer` do.
- `request.timeout.ms` and `default.api.timeout.ms` default to the **Client
  timeout (ms)** setting (5000 ms). Any matching entry in **Extra properties**
  or in an explicit **Client properties file** takes precedence.
- **Credentials for SASL**: either point **Client properties file** at your
  own `.properties` file (recommended for real credentials), or paste the whole
  `sasl.jaas.config` into **SASL JAAS config (inline, dev only)**. The inline
  value is written to an owner-only file in the plugin's private directory and
  only that path is passed to the Java process — it never appears in `argv` or
  in process output. Multiline JAAS is folded into one properties line and
  backslashes are escaped. If both are filled, the explicit file takes
  precedence. The inline value is stored in plain text with the profile:
  development only.
- **"Test and Save" runs `kafka-topics.sh --list` before persisting** — if the
  cluster does not answer, nothing is stored. There is also a standalone
  **Kafka: Test Connection** command. CLI calls open a cancellable task that
  extends the host's 5-second call budget to up to 60 seconds.
- Extra `key=value` properties (one per line) become client properties; on
  `ConsoleConsumer`/`ConsoleProducer` they are also passed as
  `--command-property` / `--consumer-property` / `--producer-property` (and as
  `-X` for `kcat`).

### 2. Topics

- **List**: `kafka-topics.sh --list` → sortable **Topics** table and the
  explorer's Topics node.
- **Details**: `kafka-topics.sh --describe` → `TopicId`, partition count,
  replication factor, configurations (`cleanup.policy`, …), leader, replicas,
  ISR and start/end offsets per partition (`kafka-get-offsets.sh --time -2/-1`,
  with a `kafka.tools.GetOffsetShell` fallback).
- **Create**: name, partitions, replication factor and optional
  `--config key=value` settings.
- **Delete**: `host::confirm` before `--delete`, warning that messages are lost.
- **Consume**: `kafka-console-consumer.sh` with `DefaultMessageFormatter`
  configured with control-character separators, so keys/values containing
  tabs, commas or newlines stay intact. Every record carries **key, value,
  timestamp (and type), headers, partition and offset**. Modes: beginning
  (`--from-beginning` / `--offset earliest`, the default), end (`latest`, only
  messages published after the click), a specific offset (with `--partition`)
  and live (a native IDE log tab, no polling).
- **Filter/search**: by value, key, header, offset range and timestamp range,
  optionally case-sensitive, in the **Messages** viewer (a sandboxed HTML page
  talking to the plugin via `openidea.postMessage`/`onMessage`). Each row has a
  local **ack** button (`✓`/`○`): it marks the message as read (persisted,
  optional **Hide acked** filter) without committing an offset to the broker.
- **Publish**: `kafka-console-producer.sh --sync` with key, value, headers
  (`parse.headers`) and an optional target partition (via `kcat -P -p`, since
  the console producer cannot pin a partition).

### 3. Partitions

- One node per partition in the explorer and a table in the **Partitions**
  detail tab: leader, replicas, ISR, start/end offsets and retained-message
  "lag" (`end − start`).
- Consume a specific partition: `--partition <n>` + `--offset`, both from the
  command and from the viewer's form.

### 4. Brokers / cluster

- `kafka-broker-api-versions.sh` → **Brokers** view: id, host, port, rack and
  availability; the controller is labelled accordingly.
- `kafka-metadata-quorum.sh describe --status` (KRaft) → `ClusterId`,
  `LeaderId`, `LeaderEpoch`, `HighWatermark` and voters. On ZooKeeper clusters
  the query fails gracefully and shows a note.

### 5. Consumer groups

- `kafka-consumer-groups.sh --list` → **Consumer Groups** view.
- `--describe --group <g>` → lag per group/topic/partition, current offset,
  log-end offset, `CONSUMER-ID` and `CLIENT-ID`, plus total lag.

### 6. User experience

- Tool windows: **Kafka** (tree explorer with a detail pane and tabs for
  partitions/configurations/live messages), **Topics**, **Messages**,
  **Connections** (form), **Brokers**, **Consumer Groups**, **Kafka CLI**
  (installation) and **Kafka Log**. All have a **Refresh** button;
  `on-schedule:60` plus the `autoRefresh` setting reload metadata
  automatically.
- **Kafka Log** (also a **Log** tab in the explorer's detail pane) keeps the
  last 80 entries as `command > status (response)`, most recent first. It is a
  left-aligned table (one command per row, no collapsed whitespace), and
  actions started from the Messages page log readable entries such as
  `consume pedidos` / `publish pedidos` in addition to the underlying java
  process. Every Kafka/java process and every plugin command is recorded with
  its exit status and redacted terminal output, so the last action's success or
  failure is visible after the status chip changes. **Clear Log** empties the
  history.
- Connection/auth/ACL/timeout errors are classified into clear messages. A
  failed Kafka connection is not silently reported as an empty topic list:
  `test_connection` checks the process exit status and stderr, and errors are
  shown with **Test Connection** / **Test and Save**. All process output is
  passed through `config::redact` before being shown.
- **Secrets**: the user's `.properties` file is referenced by path only
  (never read); sensitive keys (`password`/`secret`/`token`/`jaas`) are
  rejected in **Extra properties**; process output is redacted and secrets
  never enter `argv`. The optional inline JAAS field is for development: its
  value is stored in plain text with the connection.
- Status-bar item and *Tools* menu entries.

---

## Language

The plugin is **English by default** so it works for everyone, and follows the
IDE language when one is selected:

- The IDE has a **Language** setting (`appearance.locale`). The frontend syncs
  it to the plugin host, and `host::locale()` returns a BCP-47 tag such as
  `en`, `pt-BR`, `zh-CN`. If no language is available the host reports `en`.
- `locales/pt-BR.json` maps **English source text → Portuguese**. The IDE
  translates the manifest and view data itself (command titles, view titles,
  toolbar labels, form labels, table columns, tree labels/details, status and
  messages). Text without an entry stays English, so adding another language
  only requires dropping `locales/<tag>.json` into the package.
- Text the plugin builds itself (notifications, prompts, error messages,
  detail panes and the HTML page) is not translated by the IDE. The plugin
  reads the **same catalog** via `include_str!` and `host::locale()` in
  `src/i18n.rs`, and ships it to the HTML page in its state payload.
- A `language` setting (`auto` / `en-US` / `pt-BR`) lets a user pin the
  plugin-built text; `auto` (the default) follows the IDE.

All source strings are in English; the Portuguese strings live only in
`locales/pt-BR.json`.

## Icons

Toolbar icons use the host's real icon names (`src/ui/icons.tsx`):
`plus`, `trash`, `refresh`, `run`, `download`, `export`, `check`, `settings`,
`info`, `copy`, `server`, `folder`, `file`, `module`, `split`. The SDK docs
mention `add`/`remove`, but those names are **not** valid host icons and render
the generic file icon — `plus`/`trash` are the correct ones. The first button
of the Kafka and Connections views is `plus`, the last of Connections is
`trash`, the second of Topics is `plus`, the third `trash`, and the second of
Messages is `trash`.

## Capabilities

| Type | Capabilities |
| --- | --- |
| Required | `project.read`, `storage`, `events`, `process:curl`, `process:tar`, `process:java` |
| Optional | `process:kafka-topics.sh`, `process:kafka-console-consumer.sh`, `process:kafka-console-producer.sh`, `process:kafka-consumer-groups.sh`, `process:kafka-get-offsets.sh`, `process:kafka-broker-api-versions.sh`, `process:kafka-metadata-quorum.sh`, `process:kafka-run-class.sh`, `process:kcat`, `net:dlcdn.apache.org`, `net:downloads.apache.org`, `net:archive.apache.org`, `clipboard`, `fs.read:~` |

Missing optional capabilities degrade gracefully: the view shows a note and
the rest keeps working.

## Settings

`settings.schema.json`: `language`, `kafkaVersion`, `downloadUrl` (template
with `{version}`), `cliFlavour` (`auto` / `kafka3` / `kafka4`), `maxMessages`,
`timeoutMs`, `keepMessages`, `autoRefresh`, `keyDeserializer`,
`valueDeserializer`, `keySerializer`, `valueSerializer` and `extraClasspath`.

The serializer settings take a Java class name and are passed to
`ConsoleProducer` as `--command-property` (or `--producer-property` on Kafka
3.x). The deserializer settings are passed to `DefaultMessageFormatter` as
`--formatter-property` (or `--property` on Kafka 3.x) — this is where the
formatter actually reads `key.deserializer`/`value.deserializer`. Empty means
the CLI default; an entry in the connection's **Extra properties** wins over
the global setting; a format chosen in the Messages view wins over both global
ser/de settings. `extraClasspath` appends jars/directories to `java -cp` when
a custom class is used. A topic does **not** store a serializer: it only holds
bytes, so text published by this plugin is readable by a ByteArrayDeserializer
consumer and vice versa. See "Message formats" and "Binary payloads" below.

`cliFlavour = auto` first tries Kafka 4.x flags (`--command-config`,
`--command-property`, `--formatter-property`) and, if the CLI rejects an
option, retries with the 3.x ones (`--consumer.config`, `--producer.config`,
`--consumer-property`, `--producer-property`, `--property`). Pin `kafka3` or
`kafka4` to skip detection.

## Message formats

The Messages view has a **Format** combobox for consuming and another one for
publishing. The options mirror Kafka serializer class names:
`StringSerializer` (default), `ByteArraySerializer`, `IntegerSerializer`,
`LongSerializer`, `DoubleSerializer`, `FloatSerializer`, `BytesSerializer`,
`ByteBufferSerializer`, `VoidSerializer`, `JsonSerializer`,
`KafkaAvroSerializer` and `KafkaProtobufSerializer`. The consume combobox shows
the matching `…Deserializer` names (for example `ByteArrayDeserializer`) while
the saved value stays the canonical `…Serializer` token; the Topics view shows
each column with the correct suffix. The choice is saved per topic, so
switching topics restores what was used for each one.

- On consume, the format selects the deserializer passed to
  `--formatter-property`: the `Serializer` suffix becomes `Deserializer` and
  the class comes from `org.apache.kafka.common.serialization`, except
  `JsonSerializer` (`org.apache.kafka.connect.json`) and the two Confluent
  formats. Binary formats (`ByteArray`, `Bytes`, `ByteBuffer`, `Void`) skip the
  formatter deserializer: `ByteArrayDeserializer` would make the formatter print
  `[B@1a2b…`, while raw bytes display the UTF-8 text that was published.
  Short aliases and fully qualified class names are accepted and normalized to
  one of the names above.
- On publish, the same serializer class is passed via `--command-property`
  (Kafka 4.x) or `--producer-property` (Kafka 3.x), after the global settings
  so the per-topic choice wins. Both Kafka generations then force
  `ByteArraySerializer` inside `ConsoleProducer`, so the UTF-8 text typed in the
  form is the wire representation for every text-based format, unchanged.
- A topic only stores bytes. These formats are a client convention: a consumer
  outside the plugin must use the matching deserializer to read the same
  payload.

`KafkaAvroSerializer`, `KafkaProtobufSerializer` and `VoidSerializer` cannot
produce a message from a text field: Avro/Protobuf need Confluent jars plus a
schema registry and the console producer only accepts text; Void produces a
null value. The publish form rejects them with a clear message. To read such
payloads, set `extraClasspath` (and `schema.registry.url` in Extra properties)
or use a schema-aware client outside the plugin.

## Binary payloads

The console tools are text-oriented. UTF-8 text works with any byte-based
topic: Kafka does not attach a schema to the topic, so `StringSerializer` on
publish and `ByteArrayDeserializer` on consume are compatible. Both Kafka 4.x
and 3.x force `ByteArraySerializer` inside `ConsoleProducer`, so choosing
`ByteArraySerializer` in the form still writes the UTF-8 text typed in the
Messages view. `DefaultMessageFormatter` calls `toString()` on the
deserialized value, and `ByteArrayDeserializer` would print the Java identity
(for example `[B@1a2b…`) instead of the original text. For that reason the
binary formats (`ByteArray`, `Bytes`, `ByteBuffer`, `Void`) omit the formatter
deserializer: the raw bytes are written to stdout and the published UTF-8 text
is displayed. `StringSerializer` (mapped to `StringDeserializer` on consume)
makes the formatter decode the bytes explicitly.

## Known limitations

- Without TCP in the sandbox, **every operation depends on the external CLI**
  and a JRE. Each command starts a Java process (no persistent connection),
  so very large clusters may feel the latency of a full `describe`.
- Kafka 3.x admin tools lack `--command-property`: put TLS/extra options in
  the connection's `.properties` file in that case.
- Kafka ≥ 4.2 prints a deprecation warning for `--property` on stdout; the
  message parser discards warning lines.
- A target partition on publish requires `kcat`; without it, publish without
  a partition (the default partitioner decides by key).
- Live consumption uses a native log tab following
  `kafka-console-consumer.sh` — ideal for watching the stream; for structured
  messages and filters use a bounded read in the **Messages** view.
- `kcat` is optional and not downloaded by the installer (no official
  prebuilt binary): install it with your OS package manager.
- The installer uses the Apache HTTPS index when permitted; offline it falls
  back to the built-in catalog. Downloads prefer `https://dlcdn.apache.org/`,
  falling back to `https://downloads.apache.org/` and
  `https://archive.apache.org/dist/kafka/` for older releases. `downloadUrl`
  is configurable for private mirrors.

## Recent changes

- **Command log** is a left-aligned `Command` / `Status` / `Response` table.
  Actions started in the Messages page now log readable entries such as
  `consume pedidos` and `publish pedidos`, alongside the underlying Java
  process, with redacted output.
- **Binary payloads** from ByteArray/Bytes/ByteBuffer/Void formats display the
  published UTF-8 text instead of the formatter's Java `[B@…` identity string.
- **Beginning** is the default consume mode, so existing messages show up; a
  previously saved `end` is migrated once, then explicit choices are kept.
- Messages have a local **ack** button (`✓`/`○`) with a **Hide acked** filter.
  Acks are persisted locally and never commit an offset to the broker.
- Downloads prefer the fast **dlcdn.apache.org** CDN, fall back to
  `downloads.apache.org` / `archive.apache.org`, persist progress and resume
  interrupted transfers automatically. An existing tarball can be selected
  with the local file picker.
- The Messages format combobox shows `…Deserializer` labels while settings
  keep the canonical `…Serializer` token.
- CLI verification probes **kcat with `-V`** (not `--version`, which kcat
  rejects as an unknown argument) and **Java with `-version`**; the detected
  versions are shown in the Kafka CLI view.

## Build and install

```sh
cargo test
openidea-plugin build      # writes dist/plugin.wasm and validates the package
openidea-plugin link       # loads the folder in the IDE for development
```

Install the folder through *Settings › Plugins › Install Plugin from Disk…*.
See the [OpenIDEA plugin guide](https://github.com/l0ng-ai/openidea/blob/main/plugins/README.md).
