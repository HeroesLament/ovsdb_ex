# OVSDB

[![Hex.pm](https://img.shields.io/hexpm/v/ovsdb_ex.svg)](https://hex.pm/packages/ovsdb_ex)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/ovsdb_ex)
[![License](https://img.shields.io/hexpm/l/ovsdb_ex.svg)](https://github.com/HeroesLament/ovsdb_ex/blob/main/LICENSE)

A pure-Elixir implementation of the **Open vSwitch Database Management
Protocol** ([RFC 7047](https://www.rfc-editor.org/rfc/rfc7047)).

Provides both **client** (IDL-style in-memory replica) and **server**
(accept connections, handle RPCs) implementations for any application
that needs to speak OVSDB — configuration management of Open vSwitch
instances, SDN controller protocols like OpenSync, or any other
OVSDB-compatible database.

## Features

- **Pure Elixir, zero NIFs.** No C bindings, no ports, no shelling out
  to `ovs-vsctl`. Just `:gen_tcp` / `:ssl`, `Jason`, and GenServers.
- **Schema-agnostic.** Load any `.ovsschema` at runtime. Works with
  Open vSwitch, OpenSync, OVN, or any custom OVSDB schema.
- **RFC 7047 compliant.** Full wire-protocol support: `list_dbs`,
  `get_schema`, `transact`, `monitor`/`update`, `cancel`, `lock`,
  `echo`, and the server-side notifications.
- **IDL replica pattern.** Like `ovs.db.idl` in the official Python
  bindings, but native OTP — replica lookups are lock-free ETS reads.
- **Pluggable transport.** TCP or TLS.

## Status

Active development. The 0.2 line is feature-complete for client and
server use; APIs are stable but may evolve before 1.0. All quality
gates (format, credo strict, compile warnings-as-errors, dialyzer) pass
cleanly. Live-validated end-to-end in iex. An ExUnit test suite is
planned for a subsequent release.

## Installation

Add `ovsdb_ex` to your `mix.exs`:

```elixir
def deps do
  [
    {:ovsdb_ex, "~> 0.2"}
  ]
end
```

## Quick start

### Client: talk to an existing OVSDB server

```elixir
alias OVSDB.{ClientSession, Transaction, Operation, Condition, UUID}

{:ok, session} = ClientSession.connect("ovsdb.local", 6640)

# List databases
{:ok, dbs} = ClientSession.list_dbs(session)

# Fetch schema
{:ok, schema_json} = ClientSession.get_schema(session, "Open_vSwitch")

# Build and send a transaction
txn =
  Transaction.new("Open_vSwitch")
  |> Transaction.add(
    Operation.insert("Bridge", %{"name" => "br-int"})
  )

{:ok, [%{"uuid" => ["uuid", new_uuid]}]} =
  ClientSession.transact(session, txn)
```

### IDL: maintain a live replica

```elixir
alias OVSDB.{ClientSession, Idl, Schema, SchemaHelper}

{:ok, session} = ClientSession.connect("ovsdb.local", 6640)
{:ok, schema_json} = ClientSession.get_schema(session, "Open_vSwitch")
{:ok, schema} = Schema.parse(schema_json)

# Register interest in specific tables/columns
helper =
  SchemaHelper.new(schema)
  |> SchemaHelper.register_columns!("AWLAN_Node", ["manager_addr"])
  |> SchemaHelper.register_table!("Wifi_VIF_State")

# Start the IDL — subscribes and populates from current state
{:ok, idl} = Idl.start_link(
  session: session,
  helper: helper,
  monitor_id: "my-idl"
)

# Subscribe to change notifications
Idl.subscribe(idl, "AWLAN_Node")

# Read any time, with no roundtrip
rows = Idl.get_table(idl, "AWLAN_Node")

# Receive {:idl_changed, idl, table, :insert | :modify | :delete, uuid}
# messages whenever the replica changes
```

### Server: serve your own OVSDB-compatible database

```elixir
defmodule MyHandler do
  @behaviour OVSDB.ServerSession.Handler

  def init(_opts), do: {:ok, %{rows: %{}}}

  def handle_list_dbs(state), do: {:ok, ["My_DB"], state}

  def handle_get_schema("My_DB", state), do: {:ok, my_schema(), state}

  def handle_transact("My_DB", ops, state) do
    {results, new_state} = apply_ops(ops, state)
    {:ok, results, new_state}
  end

  defp my_schema, do: %{"name" => "My_DB", "tables" => %{...}}
  defp apply_ops(_ops, state), do: {[], state}
end

# Start a server on port 6640
{:ok, _srv} = OVSDB.Server.start_link(
  port: 6640,
  handler: MyHandler
)
```

### Working with OVSDB values

OVSDB's atomic types map to Elixir's native types. Only UUIDs need
wrapping; sets and maps get small structs for the compound types:

```elixir
alias OVSDB.{UUID, Set, Map, Value}

# Atomic types are native
42            # integer
3.14          # real
true          # boolean
"hello"       # string

# UUIDs wrap — raw strings can't be distinguished from other strings
uuid = UUID.generate()
#=> %OVSDB.UUID{value: "b7c5ef91-3a64-42d1-8a5c-f9e1d2a3b4c5"}

# Sets are unordered; 1-element sets optimize to bare value on the wire
ports = Set.new([uuid1, uuid2])

# Maps preserve ordered {k, v} entries (matching wire structure)
tags = Map.new(%{"owner" => "opensync", "role" => "ap"})

# Value.encode/1 walks any value, handling nested wrappers
Value.encode(ports)
#=> ["set", [["uuid", "..."], ["uuid", "..."]]]
```

## Architecture

The library is layered:

| Layer | Modules | Role |
|---|---|---|
| 1. Data model | `UUID`, `NamedUUID`, `Set`, `Map`, `Row`, `Value` | Typed representations of RFC 7047 values |
| 2. Protocol | `Protocol` | JSON-RPC 1.0 envelopes, wire serialization, classification |
| 3. Operations | `Condition`, `Operation`, `Transaction`, `MonitorSpec`, `Schema`, `SchemaHelper` | High-level builders for RFC 7047 operations and schemas |
| 4. Sessions | `Framer`, `Transport`, `ClientSession`, `ServerSession`, `Server`, `Idl` | Wire framing, socket ownership, request correlation, IDL replica |

Each layer uses only the layers below it. The lower layers are
usable standalone — you can build wire messages by hand, frame your
own byte streams, or construct operations without a session if you
prefer.

## Alternatives

- **[python-ovs](https://github.com/openvswitch/ovs/tree/main/python/ovs)** —
  the canonical OVSDB library from the OVS project. Reference
  implementation for the IDL pattern. Only available in Python.
- **[libovsdb](https://github.com/ovn-org/libovsdb)** — Go implementation
  used by OVN and Kubernetes CNI plugins.
- **Shelling out to `ovs-vsctl`** — works for configuration but gives
  you no real-time updates, no transactional semantics across multiple
  commands, and no server-side flexibility.

## Contributing

Issues and pull requests welcome. The library is part of a broader
effort to build SDN tooling (OpenSync controllers in particular) in
Elixir — feedback from users of other OVSDB-based protocols is especially
valuable.

## License

Apache License 2.0. See [LICENSE](LICENSE).
