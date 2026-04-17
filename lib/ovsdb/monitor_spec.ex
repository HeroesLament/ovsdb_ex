defmodule OVSDB.MonitorSpec do
  @moduledoc """
  Accumulator for the monitor requests that make up a `monitor`
  subscription.

  Per [RFC 7047 §4.1.5][rfc-monitor], a `monitor` request's params
  are:

      [<db-name>, <json-value>, <monitor-requests>]

  where `<json-value>` is an opaque identifier chosen by the client
  (echoed back in every `update` notification so clients can route
  updates to the right handler), and `<monitor-requests>` is a JSON
  object mapping table names to per-table monitor specs:

      {
        "Bridge":    {"columns": [...], "select": {...}},
        "Interface": {"columns": [...], "select": {...}}
      }

  [rfc-monitor]: https://www.rfc-editor.org/rfc/rfc7047#section-4.1.5

  ## Usage

      alias OVSDB.MonitorSpec

      spec =
        MonitorSpec.new("Open_vSwitch", "sim-node-1")
        |> MonitorSpec.watch("AWLAN_Node", columns: ["manager_addr", "redirector_addr"])
        |> MonitorSpec.watch("Wifi_VIF_Config",
             select: [:initial, :insert, :modify],
             columns: ["if_name", "enabled", "ssid"])

      params = MonitorSpec.to_params(spec)

      request = OVSDB.Protocol.request("monitor", params, request_id)

  ## `select` semantics (RFC 7047 §4.1.5)

  Each table entry may specify which categories of change to receive:

    * `:initial` — send the current state of matching rows at
      subscription time. **Only fires once.**
    * `:insert` — send new rows.
    * `:modify` — send changed rows (diff of modified columns).
    * `:delete` — send deleted rows.

  If `:select` is omitted when calling `watch/3`, **all four** are
  implicitly requested (the RFC default). When any is provided
  explicitly, the others default to `false` — so pass the full list
  of wanted categories.

  ## `columns` semantics

  If `:columns` is omitted, the server sends all columns of matching
  rows. If provided, only the listed columns plus the implicit
  `_version` column are sent. `_uuid` is always sent (it's the row
  key, not a column).

  ## No monitor_cond

  `monitor_cond` / `monitor_cond_since` (RFC 7047bis) are a proposed
  extension adding WHERE-clause filtering at the server. This module
  implements the stable `monitor` only. Conditional monitors will be
  added to a separate module when needed.
  """

  @enforce_keys [:db, :monitor_id]
  defstruct db: nil, monitor_id: nil, tables: %{}

  @type select_flag :: :initial | :insert | :modify | :delete

  @typedoc """
  Per-table subscription spec in Elixir form. Encoded via
  `to_params/1` to the wire shape.
  """
  @type table_spec :: %{
          optional(:columns) => [String.t()],
          optional(:select) => [select_flag()]
        }

  @typedoc """
  The opaque identifier the client chooses for this subscription.
  RFC 7047 says this is a "json-value" — any JSON value the client
  wants. We restrict it here to a string or non-negative integer,
  which are the forms any real client would use in practice.
  """
  @type monitor_id :: String.t() | non_neg_integer()

  @type t :: %__MODULE__{
          db: String.t(),
          monitor_id: monitor_id(),
          tables: %{optional(String.t()) => table_spec()}
        }

  @select_flags [:initial, :insert, :modify, :delete]

  @doc """
  Creates a new empty monitor spec for the given database and
  monitor id.

  The `monitor_id` must be the same value used to correlate
  subsequent `update` notifications. Session layers typically
  generate this as a unique string per subscription.

      iex> OVSDB.MonitorSpec.new("Open_vSwitch", "my-monitor")
      %OVSDB.MonitorSpec{db: "Open_vSwitch", monitor_id: "my-monitor", tables: %{}}
  """
  @spec new(String.t(), monitor_id()) :: t()
  def new(db, monitor_id)
      when is_binary(db) and (is_binary(monitor_id) or (is_integer(monitor_id) and monitor_id >= 0)) do
    %__MODULE__{db: db, monitor_id: monitor_id, tables: %{}}
  end

  @doc """
  Adds (or replaces) a monitor request for a table.

  ## Options

    * `:columns` — list of column names to subscribe to. When
      omitted, all columns are sent.
    * `:select` — list of change categories to subscribe to
      (`:initial`, `:insert`, `:modify`, `:delete`). When omitted,
      all four are requested per RFC default.

  ## Examples

      iex> MonitorSpec = OVSDB.MonitorSpec
      iex> spec = MonitorSpec.new("Open_vSwitch", "m1")
      iex> spec = MonitorSpec.watch(spec, "Bridge", columns: ["name", "ports"])
      iex> spec.tables
      %{"Bridge" => %{columns: ["name", "ports"]}}

      iex> MonitorSpec = OVSDB.MonitorSpec
      iex> spec = MonitorSpec.new("Open_vSwitch", "m1")
      iex> spec = MonitorSpec.watch(spec, "Port", select: [:insert, :delete])
      iex> spec.tables
      %{"Port" => %{select: [:insert, :delete]}}

      iex> MonitorSpec = OVSDB.MonitorSpec
      iex> spec = MonitorSpec.new("db", "m")
      iex> spec = MonitorSpec.watch(spec, "T", columns: ["a"], select: [:initial, :modify])
      iex> spec.tables
      %{"T" => %{columns: ["a"], select: [:initial, :modify]}}
  """
  @spec watch(t(), String.t(), keyword()) :: t()
  def watch(%__MODULE__{tables: tables} = spec, table, opts \\ [])
      when is_binary(table) and is_list(opts) do
    table_spec = build_table_spec(opts)
    %{spec | tables: Elixir.Map.put(tables, table, table_spec)}
  end

  @doc """
  Removes a table from the spec. Mostly useful for building specs
  programmatically where tables might be added conditionally.

      iex> MonitorSpec = OVSDB.MonitorSpec
      iex> spec = MonitorSpec.new("db", "m")
      iex> spec = MonitorSpec.watch(spec, "T", columns: ["a"])
      iex> spec = MonitorSpec.unwatch(spec, "T")
      iex> spec.tables
      %{}
  """
  @spec unwatch(t(), String.t()) :: t()
  def unwatch(%__MODULE__{tables: tables} = spec, table) when is_binary(table) do
    %{spec | tables: Elixir.Map.delete(tables, table)}
  end

  @doc """
  Returns the list of tables currently being monitored.

      iex> MonitorSpec = OVSDB.MonitorSpec
      iex> spec = MonitorSpec.new("db", "m")
      iex> spec = MonitorSpec.watch(spec, "A") |> MonitorSpec.watch("B")
      iex> MonitorSpec.tables(spec) |> Enum.sort()
      ["A", "B"]
  """
  @spec tables(t()) :: [String.t()]
  def tables(%__MODULE__{tables: tables}), do: Elixir.Map.keys(tables)

  @doc """
  Returns `true` if the spec has no tables. Subscribing to an empty
  spec is permitted by the RFC but does nothing useful.

      iex> OVSDB.MonitorSpec.new("db", "m") |> OVSDB.MonitorSpec.empty?()
      true
  """
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{tables: tables}), do: map_size(tables) == 0

  @doc """
  Converts the spec to the params list for a `monitor` request.

      iex> MonitorSpec = OVSDB.MonitorSpec
      iex> spec =
      ...>   MonitorSpec.new("Open_vSwitch", "sim-1")
      ...>   |> MonitorSpec.watch("Bridge", columns: ["name"], select: [:initial, :insert])
      iex> MonitorSpec.to_params(spec)
      [
        "Open_vSwitch",
        "sim-1",
        %{
          "Bridge" => %{
            "columns" => ["name"],
            "select" => %{
              "initial" => true,
              "insert" => true,
              "modify" => false,
              "delete" => false
            }
          }
        }
      ]
  """
  @spec to_params(t()) :: [String.t() | monitor_id() | map(), ...]
  def to_params(%__MODULE__{db: db, monitor_id: mid, tables: tables}) do
    encoded =
      for {table, ts} <- tables, into: %{} do
        {table, encode_table_spec(ts)}
      end

    [db, mid, encoded]
  end

  @doc """
  Convenience: build a complete `monitor` request in one call.

      iex> alias OVSDB.MonitorSpec
      iex> spec =
      ...>   MonitorSpec.new("Open_vSwitch", "m")
      ...>   |> MonitorSpec.watch("Bridge")
      iex> req = MonitorSpec.to_request(spec, 7)
      iex> req["method"]
      "monitor"
      iex> req["id"]
      7
  """
  @spec to_request(t(), OVSDB.Protocol.id()) :: OVSDB.Protocol.request()
  def to_request(%__MODULE__{} = spec, request_id) do
    OVSDB.Protocol.request("monitor", to_params(spec), request_id)
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp build_table_spec(opts) do
    spec = %{}

    spec =
      case Keyword.fetch(opts, :columns) do
        {:ok, cols} when is_list(cols) ->
          Enum.each(cols, fn
            c when is_binary(c) -> :ok
            other -> raise ArgumentError, "columns must be strings, got: #{inspect(other)}"
          end)

          Elixir.Map.put(spec, :columns, cols)

        :error ->
          spec
      end

    case Keyword.fetch(opts, :select) do
      {:ok, flags} when is_list(flags) ->
        Enum.each(flags, fn
          f when f in @select_flags ->
            :ok

          other ->
            raise ArgumentError,
                  "select flags must be in #{inspect(@select_flags)}, got: #{inspect(other)}"
        end)

        Elixir.Map.put(spec, :select, flags)

      :error ->
        spec
    end
  end

  # Encode one table's spec to wire form.
  defp encode_table_spec(ts) do
    wire = %{}

    wire =
      case Elixir.Map.get(ts, :columns) do
        nil -> wire
        cols -> Elixir.Map.put(wire, "columns", cols)
      end

    case Elixir.Map.get(ts, :select) do
      nil ->
        # RFC default: all four flags true. Omit the select field
        # entirely — RFC says that's equivalent to all-true and is
        # the cleanest wire form when nothing was specified.
        wire

      flags when is_list(flags) ->
        # Explicit flags: emit a full object with each flag set to
        # true if present, false if absent. Explicit is safer than
        # relying on server-side defaults.
        select_obj =
          for flag <- @select_flags, into: %{} do
            {Atom.to_string(flag), flag in flags}
          end

        Elixir.Map.put(wire, "select", select_obj)
    end
  end
end
