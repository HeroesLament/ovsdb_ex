defmodule OVSDB do
  @moduledoc """
  A pure-Elixir implementation of the Open vSwitch Database Management
  Protocol (OVSDB), as specified in [RFC 7047][rfc7047].

  [rfc7047]: https://www.rfc-editor.org/rfc/rfc7047

  ## Scope

  This library implements RFC 7047 directly. It is not bound to any
  specific schema (e.g. `Open_vSwitch.ovsschema` or OpenSync-specific
  schemas); the protocol is schema-parameterized and schemas are loaded
  at runtime.

  Use `OVSDB` if you need to:

    * Talk to `ovsdb-server` as a client (config, monitor, transact).
    * Accept OVSDB connections as a server/manager.
    * Parse, validate, or generate OVSDB wire messages.
    * Maintain an in-memory replica of a remote OVSDB database (the
      IDL pattern).

  ## Layering

  The library is organized into layers. The lowest layers are pure data
  with no process state; the upper layers are OTP processes.

    * **Layer 1 — Data model.** Elixir-native representation of OVSDB
      values on the wire. See `OVSDB.UUID`, `OVSDB.NamedUUID`,
      `OVSDB.Set`, `OVSDB.Map`, `OVSDB.Row`, `OVSDB.Value`.

    * **Layer 2 — Protocol primitives.** JSON-RPC 1.0 envelope handling
      and transact-operation builders. See `OVSDB.Protocol`.

    * **Layer 3 — Request construction.** Pure builders composing
      Layer 2 primitives. See `OVSDB.Transaction`, `OVSDB.MonitorSpec`.

    * **Layer 4 — Connection processes.** The only stateful parts of
      the library. See `OVSDB.Transport`, `OVSDB.Session`, `OVSDB.Idl`,
      `OVSDB.Server`.

  Application code typically interacts with Layer 4 (`OVSDB.Session`
  or `OVSDB.Idl` for client use, `OVSDB.Server` for accepting
  connections) and composes Layer 3 builders when issuing transactions
  or monitors.

  ## Value encoding

  OVSDB's atomic types (`integer`, `real`, `boolean`, `string`, `uuid`)
  map to Elixir's native types wherever the encoding is unambiguous.
  Only `uuid` requires a wrapper struct, because a raw UUID string is
  indistinguishable from any other string at the type level:

      %{
        "name"     => "br-lan",                     # string
        "ofport"   => 42,                            # integer
        "up"       => true,                          # boolean
        "_uuid"    => OVSDB.UUID.new("abc..."),      # uuid (wrapped)
        "ports"    => OVSDB.Set.new([u1, u2]),       # set of uuids
        "external" => OVSDB.Map.new(%{"k" => "v"})   # map of strings
      }

  Encoding these values to RFC 7047 wire form is the job of
  `OVSDB.Value.encode/1`.
  """

  @version "0.1.0"

  @doc """
  Returns the version of this library.
  """
  @spec version() :: String.t()
  def version, do: @version
end
