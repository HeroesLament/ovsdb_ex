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
- **Pluggable transport.** TCP, TLS, and Unix domain sockets.

## Status

> ⚠️ **Early development.** Layer 1 (data model) and Layer 2 (protocol
> envelopes) are implemented and tested. Layers 3 (request builders),
> 4 (connection processes), and the IDL replica are in active
> development. Version `0.1.x` is a preview release; the API will
> evolve before `1.0`.

## Installation

Add `ovsdb_ex` to your `mix.exs`:

```elixir
def deps do
  [
    {:ovsdb_ex, "~> 0.1"}
  ]
end
```

## Quick start

### Building an OVSDB request by hand

```elixir
alias OVSDB.{Protocol, UUID, Set, Value}

# Build a list_dbs request
request = Protocol.request("list_dbs", [], 1)
#=> %{"method" => "list_dbs", "params" => [], "id" => 1}

# Serialize to wire bytes
wire = Protocol.serialize(request) |> IO.iodata_to_binary()
#=> "{\"id\":1,\"method\":\"list_dbs\",\"params\":[]}"

# Parse a response from the server
{:ok, msg} = Protocol.parse(response_bytes)
{:ok, {:response, %{result: dbs}}} = Protocol.classify(msg)
```

### Working with OVSDB values

OVSDB's atomic types map to Elixir's native types. Only UUIDs need
wrapping, and sets/maps get small structs for the compound types:

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

# Maps are ordered lists of {k, v} tuples (preserving wire structure)
tags = Map.new(%{"owner" => "opensync", "role" => "ap"})

# Value.encode/1 walks any value, handling nested wrappers
Value.encode(ports)
#=> ["set", [["uuid", "..."], ["uuid", "..."]]]
```

### Planned API (coming in 0.2.x)

```elixir
alias OVSDB.{Idl, SchemaHelper, Transaction, Operation, Condition}

# Load a schema
{:ok, schema} = OVSDB.Schema.load("priv/vswitch.ovsschema")

# Register interest in specific tables/columns
helper =
  SchemaHelper.new(schema)
  |> SchemaHelper.register_columns("Bridge", ~w(name ports))
  |> SchemaHelper.register_columns("Port",   ~w(name interfaces))

# Start an IDL — maintains an in-memory replica of the remote DB
{:ok, idl} = Idl.start_link(
  remote: {:tcp, "127.0.0.1", 6640},
  schema_helper: helper
)

# Read from the replica (lock-free ETS lookup, microseconds)
bridges = Idl.list_rows(idl, "Bridge")

# Write via transaction
txn =
  Transaction.new("Open_vSwitch")
  |> Transaction.add(Operation.insert("Bridge",
       %{"name" => "br-lan"}, uuid_name: "new_br"))

{:ok, _result} = Idl.transact(idl, txn)
```

## Design philosophy

- **Pure functions where possible.** Layers 1-3 are pure Elixir term
  manipulation with no processes. Processes enter the picture only at
  Layer 4 (connection management).
- **Plain maps, not structs, for protocol messages.** A JSON-RPC
  envelope is already a map; introducing a struct wrapper adds
  ceremony without benefit.
- **Schema-parameterized, not schema-coupled.** The library works
  with any OVSDB schema. No built-in schema definitions.
- **Idiomatic OTP.** `Idl` and `Session` are GenServers. Connections
  are supervised. Failures are expected and recovered, not prevented.

## Comparison to related projects

| Project                  | Language | Status       | Notes                    |
|--------------------------|----------|--------------|--------------------------|
| `ovs.db.idl`             | Python   | Active       | Part of Open vSwitch     |
| `ryu.services.protocols.ovsdb` | Python | Active | Ryu SDN framework        |
| `ovsdbapp`               | Python   | Active       | OpenStack's wrapper      |
| `libovsdb` (Go)          | Go       | Active       | Used by ovn-kubernetes   |
| `ovsdb` (Erlang)         | Erlang   | Unmaintained | Last update 2016         |
| **`ovsdb_ex`**           | Elixir   | In progress  | This library             |

`ovsdb_ex` is the first actively-maintained OVSDB library for the
BEAM ecosystem.

## Contributing

Contributions welcome! Please:

1. Open an issue first for any non-trivial change.
2. Run `mix test`, `mix format --check-formatted`, `mix credo`, and
   `mix dialyzer` before submitting.
3. Add tests for new functionality (the sandbox scripts in this repo's
   history are a good model for comprehensive coverage).

## License

Apache 2.0. See `LICENSE`.

The Apache 2.0 license matches that of Open vSwitch itself, making it
easy to use this library alongside the Open vSwitch project.

## Acknowledgments

This library's architecture draws heavily from the official Open
vSwitch Python IDL (`ovs.db.idl`) — particularly its `SchemaHelper`,
in-memory replica, and `change_seqno` notification patterns. RFC 7047
was authored by Ben Pfaff and Bruce Davie at VMware in 2013.
