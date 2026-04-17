defmodule OVSDB.Row do
  @moduledoc """
  A row or row-subset from an OVSDB table.

  ## Wire form

  Per [RFC 7047 §5.1][1], a `<row>` is a JSON object whose members are
  column names paired with `<value>`s. Rows on the wire look like:

      {
        "name":         "br-lan",
        "ofport":       42,
        "external_ids": ["map", [["owner", "opensync"]]],
        "ports":        ["set", [["uuid", "550e..."]]]
      }

  [1]: https://www.rfc-editor.org/rfc/rfc7047#section-5.1

  ## Implicit columns

  Every OVSDB row has two implicit columns that do not appear in the
  schema:

    * `_uuid` — the row's persistent UUID (read-only, never changes).
    * `_version` — an ephemeral per-row version that changes on each
      modification and on each database restart.

  These appear in decoded rows returned by `select` and in monitor
  `update` notifications. They are NOT included in rows you build for
  `insert` or `update` operations. This module treats `_uuid` and
  `_version` as first-class struct fields (when known) rather than
  column-map entries, so application code can reach for `row.uuid`
  rather than `Row.get(row, "_uuid")`.

  ## Partial rows

  A row may be a *complete* row (all columns present) or a *subset*
  (only the columns the caller asked for via `select`'s `"columns"`
  or a monitor's `"columns"`). Both share the same struct shape;
  there's no static distinction. Callers that care should either
  know from context or check for specific columns with `get/3`.

  ## Schema-blind by design

  This module does NOT validate column values against a schema. That
  is the schema layer's job (`OVSDB.Schema.validate_row/3`).
  `Row` is pure data — a column map plus optional UUID/version
  metadata — and all values are stored in their Elixir-native
  (decoded) form, not their wire form.
  """

  defstruct [:uuid, :version, columns: %{}]

  @type t :: %__MODULE__{
          uuid: OVSDB.UUID.t() | nil,
          version: OVSDB.UUID.t() | nil,
          columns: %{optional(String.t()) => term()}
        }

  @doc """
  Creates a row from a column map.

      iex> OVSDB.Row.new(%{"name" => "br-lan", "ofport" => 42})
      %OVSDB.Row{
        uuid: nil,
        version: nil,
        columns: %{"name" => "br-lan", "ofport" => 42}
      }
  """
  @spec new(%{optional(String.t()) => term()}) :: t()
  def new(columns \\ %{}) when is_map(columns) do
    %__MODULE__{columns: columns}
  end

  @doc """
  Gets the value for `column`, or `default` if absent.

      iex> row = OVSDB.Row.new(%{"name" => "br-lan"})
      iex> OVSDB.Row.get(row, "name")
      "br-lan"
      iex> OVSDB.Row.get(row, "missing", :none)
      :none

  `_uuid` and `_version` are accessed via the struct fields, but are
  also reachable via `get/3` for convenience:

      iex> uuid = OVSDB.UUID.new("550e8400-e29b-41d4-a716-446655440000")
      iex> row = %OVSDB.Row{uuid: uuid, columns: %{}}
      iex> OVSDB.Row.get(row, "_uuid")
      %OVSDB.UUID{value: "550e8400-e29b-41d4-a716-446655440000"}
  """
  @spec get(t(), String.t(), term()) :: term()
  def get(row, column, default \\ nil)

  def get(%__MODULE__{uuid: uuid}, "_uuid", _default) when not is_nil(uuid), do: uuid
  def get(%__MODULE__{version: version}, "_version", _default) when not is_nil(version), do: version

  def get(%__MODULE__{columns: columns}, column, default) do
    Elixir.Map.get(columns, column, default)
  end

  @doc """
  Puts `value` at `column`. If `column` is `_uuid` or `_version`, the
  value is validated as a UUID struct and stored in the metadata
  field rather than the column map.

      iex> row = OVSDB.Row.new()
      iex> OVSDB.Row.put(row, "name", "br-lan")
      %OVSDB.Row{uuid: nil, version: nil, columns: %{"name" => "br-lan"}}
  """
  @spec put(t(), String.t(), term()) :: t()
  def put(%__MODULE__{} = row, "_uuid", %OVSDB.UUID{} = uuid) do
    %{row | uuid: uuid}
  end

  def put(%__MODULE__{} = row, "_version", %OVSDB.UUID{} = version) do
    %{row | version: version}
  end

  def put(%__MODULE__{columns: columns} = row, column, value) when is_binary(column) do
    %{row | columns: Elixir.Map.put(columns, column, value)}
  end

  @doc """
  Returns `true` if the row has a value for `column`.

      iex> row = OVSDB.Row.new(%{"name" => "br-lan"})
      iex> OVSDB.Row.has?(row, "name")
      true
      iex> OVSDB.Row.has?(row, "missing")
      false
  """
  @spec has?(t(), String.t()) :: boolean()
  def has?(%__MODULE__{uuid: uuid}, "_uuid"), do: not is_nil(uuid)
  def has?(%__MODULE__{version: version}, "_version"), do: not is_nil(version)
  def has?(%__MODULE__{columns: columns}, column), do: Elixir.Map.has_key?(columns, column)

  @doc """
  Returns the diff of two rows as a map of columns whose values
  differ. Used to construct minimal `update` operations.

  Only considers the `columns` map — not `_uuid`/`_version`, which
  are not client-modifiable.

      iex> old = OVSDB.Row.new(%{"name" => "br-lan", "ofport" => 1})
      iex> new = OVSDB.Row.new(%{"name" => "br-lan", "ofport" => 2, "new" => "x"})
      iex> OVSDB.Row.diff(old, new)
      %{"ofport" => 2, "new" => "x"}
  """
  @spec diff(t(), t()) :: %{optional(String.t()) => term()}
  def diff(%__MODULE__{columns: old}, %__MODULE__{columns: new}) do
    for {k, v} <- new, Elixir.Map.get(old, k) != v, into: %{}, do: {k, v}
  end

  @doc """
  Lists all column names present in the row (excluding `_uuid` and
  `_version` metadata).
  """
  @spec columns(t()) :: [String.t()]
  def columns(%__MODULE__{columns: columns}), do: Elixir.Map.keys(columns)
end
