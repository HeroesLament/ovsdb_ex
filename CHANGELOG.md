# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned

- Layer 3 request builders (`OVSDB.Transaction`, `OVSDB.Operation`,
  `OVSDB.Condition`, `OVSDB.MonitorSpec`).
- Schema parser and subscription helper (`OVSDB.Schema`,
  `OVSDB.SchemaHelper`).
- Transport, Session, IDL replica, and Server processes.

## [0.1.0] - TBD

### Added

- **Layer 1 — Data model.**
  - `OVSDB.UUID` — RFC 4122 v4 UUID wrapper with generation, parsing,
    and wire encoding.
  - `OVSDB.NamedUUID` — symbolic UUID references for use inside
    transactions.
  - `OVSDB.Set` — unordered collection with RFC 7047's 1-element-bare
    wire optimization. Schema-aware and schema-blind decoders.
  - `OVSDB.Map` — ordered key-value list with always-tagged wire form.
  - `OVSDB.Row` — row struct with `_uuid` and `_version` as
    first-class fields.
  - `OVSDB.Value` — sum-type encode/decode dispatcher for any OVSDB
    value.

- **Layer 2 — Protocol envelopes.**
  - `OVSDB.Protocol` — JSON-RPC 1.0 message builders
    (`request`, `response`, `error_response`, `notification`),
    classifier, wire serialize/parse via Jason.

[Unreleased]: https://github.com/HeroesLament/ovsdb_ex/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/HeroesLament/ovsdb_ex/releases/tag/v0.1.0
