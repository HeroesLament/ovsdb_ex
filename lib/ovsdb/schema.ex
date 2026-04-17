defmodule OVSDB.Schema do
  @moduledoc """
  Parses and exposes OVSDB schema definitions per
  [RFC 7047 §3.2][rfc-schema].

  [rfc-schema]: https://www.rfc-editor.org/rfc/rfc7047#section-3.2

  ## Purpose

  A schema describes the tables, columns, and column types of an
  OVSDB database. Clients need it to:

    * Validate rows they intend to insert or update.
    * Decode tagged values correctly (e.g. a column declared as a
      set always arrives in `["set", [...]]` form, never bare).
    * Know which columns exist and may be monitored.

  Servers need it to:

    * Reject malformed transacts.
    * Serialize their current state to the correct wire form.
    * Answer `get_schema` requests.

  This module handles parsing an `.ovsschema` JSON document (or a
  pre-parsed map from `list_dbs` / `get_schema` responses) into a
  convenient Elixir struct. It provides lookup functions for
  tables, columns, and column types.

  ## What's implemented

    * Full parsing of table and column definitions
    * Atomic, set, and map column kinds
    * `min`/`max` cardinality (including `"unlimited"`)
    * `key` / `value` type constraints as carried metadata
    * Basic shape validation of rows against column types

  ## What's NOT implemented yet

    * Enum validation (RFC 7047 §3.2: `"enum"` constraint)
    * String min/max length enforcement
    * Integer/real min/max enforcement
    * Strong reference constraint enforcement (`refType: "strong"`)
    * Garbage collection of unreferenced rows (server-side concern)

  These are carried as metadata on the column struct and available
  via `Column.constraints/1` but are not actively enforced by
  `validate_row/3`. Add them as you need them.

  ## Wire form of the schema document

  A schema JSON object at the top level looks like:

      {
        "name": "Open_vSwitch",
        "version": "8.3.0",
        "cksum": "12345678 5000",
        "tables": {
          "Bridge": {
            "columns": {
              "name":  {"type": "string"},
              "ports": {"type": {"key": {"type": "uuid", "refTable": "Port"},
                                  "min": 0, "max": "unlimited"}}
            },
            "isRoot": true,
            "indexes": [["name"]]
          },
          "Port": {...}
        }
      }
  """

  defmodule Column do
    @moduledoc """
    A parsed column definition.
    """

    @enforce_keys [:name, :kind, :key_type]
    defstruct [:name, :kind, :key_type, :value_type, :min, :max, :mutable, :ephemeral]

    @type atomic_type :: :integer | :real | :boolean | :string | :uuid

    @typedoc """
    A typed value carrier. For atomic types it's just the atom; for
    reference types (`:uuid` pointing at another table) it carries
    the target table name and ref strength.
    """
    @type type_spec ::
            atomic_type()
            | {:ref, target :: String.t(), strength :: :strong | :weak}
            | {:enum, atomic_type(), [term()]}
            | {:ranged, :integer | :real, min :: number() | nil, max :: number() | nil}
            | {:bounded_string, min_len :: non_neg_integer() | nil,
               max_len :: non_neg_integer() | nil}

    @type kind :: :atomic | :set | :map

    @type t :: %__MODULE__{
            name: String.t(),
            kind: kind(),
            key_type: type_spec(),
            value_type: type_spec() | nil,
            min: non_neg_integer(),
            max: non_neg_integer() | :unlimited,
            mutable: boolean(),
            ephemeral: boolean()
          }

    @doc """
    Returns `true` if the column holds a set of values (`min`/`max`
    allow more than one).
    """
    @spec set?(t()) :: boolean()
    def set?(%__MODULE__{kind: :set}), do: true
    def set?(%__MODULE__{}), do: false

    @doc "Returns `true` if the column holds a map."
    @spec map?(t()) :: boolean()
    def map?(%__MODULE__{kind: :map}), do: true
    def map?(%__MODULE__{}), do: false

    @doc "Returns `true` if the column is a scalar (single required value)."
    @spec atomic?(t()) :: boolean()
    def atomic?(%__MODULE__{kind: :atomic}), do: true
    def atomic?(%__MODULE__{}), do: false

    @doc "Returns `true` if the column may be optionally absent (min == 0, max == 1)."
    @spec optional?(t()) :: boolean()
    def optional?(%__MODULE__{min: 0, max: 1}), do: true
    def optional?(%__MODULE__{}), do: false

    @doc """
    Returns a summary of the constraints carried by the key type.
    Used by client code that wants to enforce constraints the core
    `validate_row/3` currently skips.
    """
    @spec constraints(t()) :: %{key: type_spec(), value: type_spec() | nil}
    def constraints(%__MODULE__{key_type: k, value_type: v}), do: %{key: k, value: v}
  end

  defmodule Table do
    @moduledoc """
    A parsed table definition.
    """

    @enforce_keys [:name, :columns]
    defstruct [:name, :columns, :is_root, :indexes, :max_rows]

    @type t :: %__MODULE__{
            name: String.t(),
            columns: %{optional(String.t()) => Column.t()},
            is_root: boolean(),
            indexes: [[String.t()]],
            max_rows: non_neg_integer() | nil
          }

    @doc "Fetches a column by name."
    @spec column(t(), String.t()) :: {:ok, Column.t()} | :error
    def column(%__MODULE__{columns: cols}, name) do
      Elixir.Map.fetch(cols, name)
    end

    @doc "Returns the sorted list of column names in this table."
    @spec column_names(t()) :: [String.t()]
    def column_names(%__MODULE__{columns: cols}),
      do: cols |> Elixir.Map.keys() |> Enum.sort()
  end

  @enforce_keys [:name, :tables]
  defstruct [:name, :version, :cksum, :tables]

  @type t :: %__MODULE__{
          name: String.t(),
          version: String.t() | nil,
          cksum: String.t() | nil,
          tables: %{optional(String.t()) => Table.t()}
        }

  # ---------------------------------------------------------------------------
  # Public API — parsing
  # ---------------------------------------------------------------------------

  @doc """
  Parses a pre-decoded schema JSON map into a `Schema` struct.

  Returns `{:ok, schema}` on success or `{:error, reason}` for
  missing required fields or malformed type definitions.

      iex> json = %{
      ...>   "name" => "Open_vSwitch",
      ...>   "version" => "8.3.0",
      ...>   "tables" => %{
      ...>     "Bridge" => %{
      ...>       "columns" => %{"name" => %{"type" => "string"}}
      ...>     }
      ...>   }
      ...> }
      iex> {:ok, schema} = OVSDB.Schema.parse(json)
      iex> schema.name
      "Open_vSwitch"
      iex> Map.keys(schema.tables)
      ["Bridge"]
  """
  @spec parse(map()) :: {:ok, t()} | {:error, term()}
  def parse(%{"name" => name, "tables" => tables_json} = doc)
      when is_binary(name) and is_map(tables_json) do
    with {:ok, tables} <- parse_tables(tables_json) do
      {:ok,
       %__MODULE__{
         name: name,
         version: Elixir.Map.get(doc, "version"),
         cksum: Elixir.Map.get(doc, "cksum"),
         tables: tables
       }}
    end
  end

  def parse(doc) when is_map(doc) do
    missing =
      ["name", "tables"]
      |> Enum.reject(&Elixir.Map.has_key?(doc, &1))

    {:error, {:missing_fields, missing}}
  end

  def parse(other), do: {:error, {:not_a_map, other}}

  @doc """
  Parses a JSON string into a schema. Convenience wrapper around
  `Jason.decode/1` + `parse/1`.

      iex> json = ~s({"name":"db","tables":{}})
      iex> {:ok, schema} = OVSDB.Schema.parse_string(json)
      iex> schema.name
      "db"
  """
  @spec parse_string(binary()) :: {:ok, t()} | {:error, term()}
  def parse_string(binary) when is_binary(binary) do
    with {:ok, doc} <- Jason.decode(binary) do
      parse(doc)
    end
  end

  # ---------------------------------------------------------------------------
  # Public API — lookups
  # ---------------------------------------------------------------------------

  @doc """
  Fetches a table by name. Returns `{:ok, table}` or `:error`.

      iex> {:ok, schema} = OVSDB.Schema.parse(%{"name" => "db",
      ...>   "tables" => %{"T" => %{"columns" => %{"c" => %{"type" => "string"}}}}})
      iex> {:ok, table} = OVSDB.Schema.table(schema, "T")
      iex> table.name
      "T"
      iex> OVSDB.Schema.table(schema, "Missing")
      :error
  """
  @spec table(t(), String.t()) :: {:ok, Table.t()} | :error
  def table(%__MODULE__{tables: tables}, name), do: Elixir.Map.fetch(tables, name)

  @doc "Returns the sorted list of table names in the schema."
  @spec table_names(t()) :: [String.t()]
  def table_names(%__MODULE__{tables: tables}),
    do: tables |> Elixir.Map.keys() |> Enum.sort()

  @doc """
  Returns the column definition for `table_name`.`column_name`, or
  `:error` if either is missing.
  """
  @spec column(t(), String.t(), String.t()) :: {:ok, Column.t()} | :error
  def column(%__MODULE__{} = schema, table_name, column_name) do
    with {:ok, table} <- table(schema, table_name) do
      Table.column(table, column_name)
    end
  end

  # ---------------------------------------------------------------------------
  # Public API — row validation (shape only; see module doc)
  # ---------------------------------------------------------------------------

  @doc """
  Validates a row's columns against a table's schema.

  Checks that every column in the row exists in the table. Type
  checking is shape-level only: scalar columns must receive a scalar
  value, set columns must receive `OVSDB.Set.t()` or a bare scalar
  (the 1-element set short form), map columns must receive
  `OVSDB.Map.t()`.

  Returns `:ok` on success or `{:error, reason}` with a descriptive
  tag. Constraint-level validation (enum, length, range) is not
  performed — see module doc.

      iex> {:ok, schema} = OVSDB.Schema.parse(%{
      ...>   "name" => "db",
      ...>   "tables" => %{"T" => %{"columns" => %{
      ...>     "name" => %{"type" => "string"},
      ...>     "count" => %{"type" => "integer"}
      ...>   }}}
      ...> })
      iex> OVSDB.Schema.validate_row(schema, "T", %{"name" => "x", "count" => 3})
      :ok

      iex> {:ok, schema} = OVSDB.Schema.parse(%{
      ...>   "name" => "db", "tables" => %{"T" => %{"columns" => %{"n" => %{"type" => "string"}}}}
      ...> })
      iex> OVSDB.Schema.validate_row(schema, "T", %{"wrong" => 1})
      {:error, {:unknown_column, "T", "wrong"}}

      iex> {:ok, schema} = OVSDB.Schema.parse(%{
      ...>   "name" => "db", "tables" => %{}
      ...> })
      iex> OVSDB.Schema.validate_row(schema, "Nope", %{})
      {:error, {:unknown_table, "Nope"}}
  """
  @spec validate_row(t(), String.t(), map()) :: :ok | {:error, term()}
  def validate_row(%__MODULE__{} = schema, table_name, row) when is_map(row) do
    case table(schema, table_name) do
      :error ->
        {:error, {:unknown_table, table_name}}

      {:ok, table} ->
        validate_columns(table, row)
    end
  end

  defp validate_columns(%Table{} = table, row) do
    Enum.reduce_while(row, :ok, fn {col_name, value}, _ ->
      validate_one_column(table, col_name, value)
    end)
  end

  defp validate_one_column(table, col_name, value) do
    case Table.column(table, col_name) do
      :error ->
        {:halt, {:error, {:unknown_column, table.name, col_name}}}

      {:ok, column} ->
        case validate_value(column, value) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, {:bad_value, table.name, col_name, reason}}}
        end
    end
  end

  defp validate_value(%Column{kind: :atomic, key_type: kt}, value) do
    validate_atomic(kt, value)
  end

  defp validate_value(%Column{kind: :set}, %OVSDB.Set{}), do: :ok

  defp validate_value(%Column{kind: :set, key_type: kt}, bare) do
    # A bare value is allowed as a 1-element set short form.
    case validate_atomic(kt, bare) do
      :ok -> :ok
      _ -> {:error, :expected_set_or_bare_value}
    end
  end

  defp validate_value(%Column{kind: :map}, %OVSDB.Map{}), do: :ok
  defp validate_value(%Column{kind: :map}, _), do: {:error, :expected_map}

  defp validate_atomic(:integer, v) when is_integer(v), do: :ok
  defp validate_atomic(:real, v) when is_float(v) or is_integer(v), do: :ok
  defp validate_atomic(:boolean, v) when is_boolean(v), do: :ok
  defp validate_atomic(:string, v) when is_binary(v), do: :ok
  defp validate_atomic(:uuid, %OVSDB.UUID{}), do: :ok
  defp validate_atomic(:uuid, %OVSDB.NamedUUID{}), do: :ok

  # Ref, enum, ranged, and bounded_string wrap an atomic type — delegate.
  defp validate_atomic({:ref, _table, _strength}, v), do: validate_atomic(:uuid, v)
  defp validate_atomic({:enum, atomic, _values}, v), do: validate_atomic(atomic, v)
  defp validate_atomic({:ranged, atomic, _min, _max}, v), do: validate_atomic(atomic, v)
  defp validate_atomic({:bounded_string, _min, _max}, v), do: validate_atomic(:string, v)

  defp validate_atomic(_, _), do: {:error, :type_mismatch}

  # ---------------------------------------------------------------------------
  # Private — schema parsing
  # ---------------------------------------------------------------------------

  defp parse_tables(tables_json) do
    Enum.reduce_while(tables_json, {:ok, %{}}, fn {table_name, table_json}, {:ok, acc} ->
      case parse_table(table_name, table_json) do
        {:ok, table} -> {:cont, {:ok, Elixir.Map.put(acc, table_name, table)}}
        {:error, reason} -> {:halt, {:error, {:in_table, table_name, reason}}}
      end
    end)
  end

  defp parse_table(name, %{"columns" => cols_json} = tj) when is_map(cols_json) do
    with {:ok, columns} <- parse_columns(cols_json) do
      {:ok,
       %Table{
         name: name,
         columns: columns,
         is_root: Elixir.Map.get(tj, "isRoot", false),
         indexes: Elixir.Map.get(tj, "indexes", []),
         max_rows: Elixir.Map.get(tj, "maxRows")
       }}
    end
  end

  defp parse_table(_name, _), do: {:error, :missing_columns}

  defp parse_columns(cols_json) do
    Enum.reduce_while(cols_json, {:ok, %{}}, fn {col_name, col_json}, {:ok, acc} ->
      case parse_column(col_name, col_json) do
        {:ok, col} -> {:cont, {:ok, Elixir.Map.put(acc, col_name, col)}}
        {:error, reason} -> {:halt, {:error, {:in_column, col_name, reason}}}
      end
    end)
  end

  defp parse_column(name, %{"type" => type_json} = cj) do
    with {:ok, {kind, key_type, value_type, min, max}} <- parse_type(type_json) do
      {:ok,
       %Column{
         name: name,
         kind: kind,
         key_type: key_type,
         value_type: value_type,
         min: min,
         max: max,
         mutable: Elixir.Map.get(cj, "mutable", true),
         ephemeral: Elixir.Map.get(cj, "ephemeral", false)
       }}
    end
  end

  defp parse_column(_name, _), do: {:error, :missing_type}

  # Type can be: a string (atomic shorthand), or an object with "key"
  # (and optionally value/min/max).
  defp parse_type(type_string) when is_binary(type_string) do
    with {:ok, atomic} <- parse_atomic_string(type_string) do
      {:ok, {:atomic, atomic, nil, 1, 1}}
    end
  end

  defp parse_type(%{"key" => key_json} = tj) do
    min = Elixir.Map.get(tj, "min", 1)
    max = parse_max(Elixir.Map.get(tj, "max", 1))
    value_json = Elixir.Map.get(tj, "value")

    with {:ok, key_type} <- parse_key_or_value(key_json),
         {:ok, value_type} <- parse_optional_key_or_value(value_json) do
      kind = determine_kind(min, max, value_type)
      {:ok, {kind, key_type, value_type, min, max}}
    end
  end

  defp parse_type(other), do: {:error, {:bad_type, other}}

  defp parse_max("unlimited"), do: :unlimited
  defp parse_max(n) when is_integer(n) and n >= 1, do: n
  defp parse_max(other), do: {:error, {:bad_max, other}}

  defp determine_kind(_min, _max, value_type) when not is_nil(value_type), do: :map
  defp determine_kind(1, 1, nil), do: :atomic
  defp determine_kind(_, _, nil), do: :set

  defp parse_optional_key_or_value(nil), do: {:ok, nil}
  defp parse_optional_key_or_value(json), do: parse_key_or_value(json)

  defp parse_key_or_value(type_string) when is_binary(type_string) do
    parse_atomic_string(type_string)
  end

  defp parse_key_or_value(%{"type" => type_string} = tj) when is_binary(type_string) do
    with {:ok, atomic} <- parse_atomic_string(type_string) do
      wrap_constraints(atomic, tj)
    end
  end

  defp parse_key_or_value(other), do: {:error, {:bad_key_or_value, other}}

  # Wrap an atomic type with any additional constraints found in the
  # object. Precedence: enum > ref > ranged > bounded_string > atomic.
  defp wrap_constraints(atomic, tj) do
    cond do
      Elixir.Map.has_key?(tj, "enum") -> wrap_enum(atomic, tj)
      Elixir.Map.has_key?(tj, "refTable") -> wrap_ref(tj)
      ranged?(atomic, tj) -> wrap_ranged(atomic, tj)
      bounded_string?(atomic, tj) -> wrap_bounded_string(tj)
      true -> {:ok, atomic}
    end
  end

  defp wrap_enum(atomic, tj) do
    {:ok, {:enum, atomic, parse_enum(Elixir.Map.get(tj, "enum"))}}
  end

  defp wrap_ref(tj) do
    target = Elixir.Map.get(tj, "refTable")
    strength = parse_ref_type(Elixir.Map.get(tj, "refType", "strong"))
    {:ok, {:ref, target, strength}}
  end

  defp wrap_ranged(atomic, tj) do
    min_v = Elixir.Map.get(tj, "minInteger") || Elixir.Map.get(tj, "minReal")
    max_v = Elixir.Map.get(tj, "maxInteger") || Elixir.Map.get(tj, "maxReal")
    {:ok, {:ranged, atomic, min_v, max_v}}
  end

  defp wrap_bounded_string(tj) do
    {:ok, {:bounded_string, Elixir.Map.get(tj, "minLength"), Elixir.Map.get(tj, "maxLength")}}
  end

  defp ranged?(atomic, tj) do
    atomic in [:integer, :real] and
      (Elixir.Map.has_key?(tj, "minInteger") or Elixir.Map.has_key?(tj, "maxInteger") or
         Elixir.Map.has_key?(tj, "minReal") or Elixir.Map.has_key?(tj, "maxReal"))
  end

  defp bounded_string?(atomic, tj) do
    atomic == :string and
      (Elixir.Map.has_key?(tj, "minLength") or Elixir.Map.has_key?(tj, "maxLength"))
  end

  defp parse_atomic_string("integer"), do: {:ok, :integer}
  defp parse_atomic_string("real"), do: {:ok, :real}
  defp parse_atomic_string("boolean"), do: {:ok, :boolean}
  defp parse_atomic_string("string"), do: {:ok, :string}
  defp parse_atomic_string("uuid"), do: {:ok, :uuid}
  defp parse_atomic_string(other), do: {:error, {:unknown_atomic_type, other}}

  defp parse_enum(["set", values]) when is_list(values), do: values
  defp parse_enum(single), do: [single]

  defp parse_ref_type("strong"), do: :strong
  defp parse_ref_type("weak"), do: :weak
  defp parse_ref_type(_), do: :strong
end
