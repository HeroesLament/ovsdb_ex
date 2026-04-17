defmodule OVSDB.Operation do
  @moduledoc """
  Builders for the ten database operations defined by
  [RFC 7047 §5.2][rfc-ops]. Each builder returns a plain
  string-keyed Elixir map — the shape that goes into the `"params"`
  array of a `transact` request.

  [rfc-ops]: https://www.rfc-editor.org/rfc/rfc7047#section-5.2

  ## Operations

    * `insert/3` — §5.2.1
    * `select/3` — §5.2.2
    * `update/3` — §5.2.3
    * `mutate/3` — §5.2.4
    * `delete/2` — §5.2.5
    * `wait/4` — §5.2.6
    * `commit/1` — §5.2.7
    * `abort/0` — §5.2.8
    * `comment/1` — §5.2.9
    * `assert/1` — §5.2.10

  ## Value encoding

  Every value in a row or mutation is walked through
  `OVSDB.Value.encode/1` so that wrapped `OVSDB.UUID`, `OVSDB.NamedUUID`,
  `OVSDB.Set`, and `OVSDB.Map` structs produce their correct wire
  forms. Atomic values pass through unchanged.

  ## Row inputs

  Where an operation accepts a row (`insert`, `update`, `wait`), both
  of these are accepted:

    * An `OVSDB.Row.t()` struct (its `columns` field is used; `_uuid`
      and `_version` metadata are ignored — clients don't write those).
    * A plain `%{column_name => value}` map.

  Column values may be any `OVSDB.Value.value()` (atomic, Set, Map,
  UUID, NamedUUID) and are encoded to wire form.

  ## No schema validation

  These builders produce well-formed wire shapes but do not validate
  column names, types, or constraints against a schema — that's the
  job of `OVSDB.Schema.validate_row/3`. Wrong column types will produce
  a server-side error at transaction time.
  """

  alias OVSDB.{Condition, Row, Value}

  @type table :: String.t()
  @type column :: String.t()

  @typedoc """
  A row as passed to an operation builder. Either a `Row` struct
  (metadata stripped) or a raw column map. Values are Elixir-native
  and will be encoded via `OVSDB.Value.encode/1`.
  """
  @type row_input :: Row.t() | %{optional(column()) => Value.value()}

  @typedoc """
  The wire-form representation of an operation — a JSON object with
  `"op"` plus operation-specific fields.
  """
  @type t :: %{required(String.t()) => term()}

  # ---------------------------------------------------------------------------
  # §5.2.1 — insert
  # ---------------------------------------------------------------------------

  @doc """
  Builds an `insert` operation.

  A `uuid_name:` option gives the inserted row a symbolic name that
  other operations in the same transaction can reference via
  `OVSDB.NamedUUID` — see RFC 7047 §5.1.

      iex> OVSDB.Operation.insert("Bridge", %{"name" => "br-lan"})
      %{"op" => "insert", "table" => "Bridge", "row" => %{"name" => "br-lan"}}

      iex> OVSDB.Operation.insert("Bridge", %{"name" => "br-lan"}, uuid_name: "new_br")
      %{
        "op" => "insert",
        "table" => "Bridge",
        "row" => %{"name" => "br-lan"},
        "uuid-name" => "new_br"
      }
  """
  @spec insert(table(), row_input(), keyword()) :: t()
  def insert(table, row, opts \\ []) when is_binary(table) do
    base = %{
      "op" => "insert",
      "table" => table,
      "row" => encode_row(row)
    }

    case Keyword.get(opts, :uuid_name) do
      nil -> base
      name when is_binary(name) -> Elixir.Map.put(base, "uuid-name", name)
    end
  end

  # ---------------------------------------------------------------------------
  # §5.2.2 — select
  # ---------------------------------------------------------------------------

  @doc """
  Builds a `select` operation.

  The third argument is an optional list of column names to project.
  When omitted (or `nil`), the server returns all columns of matching
  rows.

      iex> OVSDB.Operation.select("Bridge", [OVSDB.Condition.eq("name", "br-lan")])
      %{
        "op" => "select",
        "table" => "Bridge",
        "where" => [["name", "==", "br-lan"]]
      }

      iex> OVSDB.Operation.select("Bridge",
      ...>   [OVSDB.Condition.eq("name", "br-lan")],
      ...>   ["_uuid", "ports"])
      %{
        "op" => "select",
        "table" => "Bridge",
        "where" => [["name", "==", "br-lan"]],
        "columns" => ["_uuid", "ports"]
      }
  """
  @spec select(table(), [Condition.t()], [column()] | nil) :: t()
  def select(table, where, columns \\ nil) when is_binary(table) and is_list(where) do
    base = %{
      "op" => "select",
      "table" => table,
      "where" => Condition.encode_all(where)
    }

    case columns do
      nil -> base
      list when is_list(list) -> Elixir.Map.put(base, "columns", list)
    end
  end

  # ---------------------------------------------------------------------------
  # §5.2.3 — update
  # ---------------------------------------------------------------------------

  @doc """
  Builds an `update` operation.

      iex> OVSDB.Operation.update("AWLAN_Node",
      ...>   [OVSDB.Condition.eq("serial_number", "SIM-DEADBEEF")],
      ...>   %{"manager_addr" => "ssl:mgr.osync.local:443"})
      %{
        "op" => "update",
        "table" => "AWLAN_Node",
        "where" => [["serial_number", "==", "SIM-DEADBEEF"]],
        "row" => %{"manager_addr" => "ssl:mgr.osync.local:443"}
      }
  """
  @spec update(table(), [Condition.t()], row_input()) :: t()
  def update(table, where, row) when is_binary(table) and is_list(where) do
    %{
      "op" => "update",
      "table" => table,
      "where" => Condition.encode_all(where),
      "row" => encode_row(row)
    }
  end

  # ---------------------------------------------------------------------------
  # §5.2.4 — mutate
  # ---------------------------------------------------------------------------

  @typedoc """
  An atom representing one of the RFC 7047 §5.1 mutator functions:

    * Arithmetic (integer/real only): `:add`, `:sub`, `:mul`, `:div`, `:mod`
    * Set/map: `:insert`, `:delete`
  """
  @type mutator ::
          :add | :sub | :mul | :div | :mod | :insert | :delete

  @type mutation :: {column(), mutator(), Value.value()}

  @mutators %{
    add: "+=",
    sub: "-=",
    mul: "*=",
    div: "/=",
    mod: "%=",
    insert: "insert",
    delete: "delete"
  }

  @doc """
  Returns the list of atom mutators supported by `mutate/3`.

      iex> OVSDB.Operation.mutators() |> Enum.sort()
      [:add, :delete, :div, :insert, :mod, :mul, :sub]
  """
  @spec mutators() :: [mutator()]
  def mutators, do: Elixir.Map.keys(@mutators)

  @doc """
  Returns the RFC 7047 wire string for a mutator atom.

      iex> OVSDB.Operation.mutator_string(:add)
      "+="

      iex> OVSDB.Operation.mutator_string(:insert)
      "insert"
  """
  @spec mutator_string(mutator()) :: String.t()
  def mutator_string(mut) when is_atom(mut) do
    case Elixir.Map.fetch(@mutators, mut) do
      {:ok, s} -> s
      :error -> raise ArgumentError, "unknown mutator: #{inspect(mut)}"
    end
  end

  @doc """
  Builds a `mutate` operation.

  Each mutation is `{column, mutator_atom, value}`. See `mutators/0`
  for the supported atoms.

      iex> mutations = [
      ...>   {"client_count", :add, 1},
      ...>   {"ports", :insert, OVSDB.Set.new([OVSDB.UUID.new("550e8400-e29b-41d4-a716-446655440000")])}
      ...> ]
      iex> OVSDB.Operation.mutate("Bridge",
      ...>   [OVSDB.Condition.eq("name", "br-lan")],
      ...>   mutations)
      %{
        "op" => "mutate",
        "table" => "Bridge",
        "where" => [["name", "==", "br-lan"]],
        "mutations" => [
          ["client_count", "+=", 1],
          ["ports", "insert", ["uuid", "550e8400-e29b-41d4-a716-446655440000"]]
        ]
      }
  """
  @spec mutate(table(), [Condition.t()], [mutation()]) :: t()
  def mutate(table, where, mutations)
      when is_binary(table) and is_list(where) and is_list(mutations) do
    %{
      "op" => "mutate",
      "table" => table,
      "where" => Condition.encode_all(where),
      "mutations" => Enum.map(mutations, &encode_mutation/1)
    }
  end

  @spec encode_mutation(mutation()) :: [String.t() | Value.wire(), ...]
  defp encode_mutation({column, mut, value}) when is_binary(column) and is_atom(mut) do
    [column, mutator_string(mut), Value.encode(value)]
  end

  # ---------------------------------------------------------------------------
  # §5.2.5 — delete
  # ---------------------------------------------------------------------------

  @doc """
  Builds a `delete` operation.

      iex> OVSDB.Operation.delete("Bridge",
      ...>   [OVSDB.Condition.eq("name", "br-lan")])
      %{
        "op" => "delete",
        "table" => "Bridge",
        "where" => [["name", "==", "br-lan"]]
      }
  """
  @spec delete(table(), [Condition.t()]) :: t()
  def delete(table, where) when is_binary(table) and is_list(where) do
    %{
      "op" => "delete",
      "table" => table,
      "where" => Condition.encode_all(where)
    }
  end

  # ---------------------------------------------------------------------------
  # §5.2.6 — wait
  # ---------------------------------------------------------------------------

  @typedoc """
  The `until` field of a `wait` operation. `"=="` blocks until all
  rows match the given values; `"!="` blocks until none match.
  """
  @type wait_until :: :eq | :ne

  @doc """
  Builds a `wait` operation. Per RFC 7047 §5.2.6, this blocks the
  transaction until the rows matching `where` have `columns` equal
  (`until: :eq`) or unequal (`until: :ne`) to the given `rows`.

  ## Options

    * `:until` — `:eq` (default) or `:ne`.
    * `:timeout` — milliseconds to wait before failing. Default `0`
      means the condition is checked immediately and fails if not
      met. The RFC permits `0` to mean "fail immediately if not met"
      rather than "wait forever" — consistent with its semantics.

  ## Example

      iex> OVSDB.Operation.wait("AWLAN_Node",
      ...>   [OVSDB.Condition.eq("serial_number", "SIM-DEADBEEF")],
      ...>   ["applied_generation"],
      ...>   [%{"applied_generation" => 7}],
      ...>   until: :eq,
      ...>   timeout: 5_000)
      %{
        "op" => "wait",
        "table" => "AWLAN_Node",
        "where" => [["serial_number", "==", "SIM-DEADBEEF"]],
        "columns" => ["applied_generation"],
        "until" => "==",
        "rows" => [%{"applied_generation" => 7}],
        "timeout" => 5_000
      }
  """
  @spec wait(table(), [Condition.t()], [column()], [row_input()], keyword()) :: t()
  def wait(table, where, columns, rows, opts \\ [])
      when is_binary(table) and is_list(where) and is_list(columns) and is_list(rows) do
    until =
      case Keyword.get(opts, :until, :eq) do
        :eq -> "=="
        :ne -> "!="
        other -> raise ArgumentError, "wait/5 :until must be :eq or :ne, got: #{inspect(other)}"
      end

    timeout = Keyword.get(opts, :timeout, 0)

    unless is_integer(timeout) and timeout >= 0 do
      raise ArgumentError, "wait/5 :timeout must be a non-negative integer, got: #{inspect(timeout)}"
    end

    %{
      "op" => "wait",
      "table" => table,
      "where" => Condition.encode_all(where),
      "columns" => columns,
      "until" => until,
      "rows" => Enum.map(rows, &encode_row/1),
      "timeout" => timeout
    }
  end

  # ---------------------------------------------------------------------------
  # §5.2.7 — commit
  # ---------------------------------------------------------------------------

  @doc """
  Builds a `commit` operation. When `durable?` is `true`, the server
  will not return success until the transaction is written to
  non-volatile storage (RFC 7047 §1.2's "Durable").

      iex> OVSDB.Operation.commit()
      %{"op" => "commit", "durable" => false}

      iex> OVSDB.Operation.commit(true)
      %{"op" => "commit", "durable" => true}
  """
  @spec commit(boolean()) :: t()
  def commit(durable? \\ false) when is_boolean(durable?) do
    %{"op" => "commit", "durable" => durable?}
  end

  # ---------------------------------------------------------------------------
  # §5.2.8 — abort
  # ---------------------------------------------------------------------------

  @doc """
  Builds an `abort` operation. Causes the transaction to fail
  unconditionally — useful for testing rollback behavior.

      iex> OVSDB.Operation.abort()
      %{"op" => "abort"}
  """
  @spec abort() :: t()
  def abort, do: %{"op" => "abort"}

  # ---------------------------------------------------------------------------
  # §5.2.9 — comment
  # ---------------------------------------------------------------------------

  @doc """
  Builds a `comment` operation. Comments appear in the server's
  transaction log and are useful for correlating client actions with
  server-side audit trails.

      iex> OVSDB.Operation.comment("applying manager.addr for node X")
      %{"op" => "comment", "comment" => "applying manager.addr for node X"}
  """
  @spec comment(String.t()) :: t()
  def comment(text) when is_binary(text) do
    %{"op" => "comment", "comment" => text}
  end

  # ---------------------------------------------------------------------------
  # §5.2.10 — assert
  # ---------------------------------------------------------------------------

  @doc """
  Builds an `assert` operation. Requires the client to currently hold
  the named lock for the transaction to proceed — see RFC 7047 §4.1.8
  for lock semantics.

      iex> OVSDB.Operation.assert_lock("my_lock")
      %{"op" => "assert", "lock" => "my_lock"}

  Note: this function is named `assert_lock` rather than `assert` to
  avoid shadowing the `assert` macro from `ExUnit.Assertions`, which
  callers might inadvertently reach for in test contexts.
  """
  @spec assert_lock(String.t()) :: t()
  def assert_lock(lock) when is_binary(lock) do
    %{"op" => "assert", "lock" => lock}
  end

  # ---------------------------------------------------------------------------
  # Shared helpers
  # ---------------------------------------------------------------------------

  # Normalize a row input to a wire-ready column map.
  defp encode_row(%Row{columns: columns}), do: encode_row(columns)

  defp encode_row(map) when is_map(map) do
    for {k, v} <- map, into: %{}, do: {k, Value.encode(v)}
  end

  defp encode_row(other) do
    raise ArgumentError,
          "row must be an OVSDB.Row or a column map, got: #{inspect(other)}"
  end
end
