defmodule OVSDB.SchemaHelper do
  @moduledoc """
  Accumulator for "which tables and columns does this client care
  about?" — used to build a filtered `OVSDB.Schema` and the
  corresponding `OVSDB.MonitorSpec` subscription.

  ## Why this exists

  A real OVSDB schema (OpenSync, Open vSwitch, OVN) defines dozens
  of tables and hundreds of columns. A client typically cares about
  a small subset. `SchemaHelper` is the builder that captures which
  subset, in a form that drives both the IDL replica (what to cache
  in memory) and the `monitor` request (what to ask the server to
  send updates for).

  The pattern mirrors `ovs.db.idl.SchemaHelper` from the Python
  reference implementation — load a full schema, register interest,
  then produce the filtered artifacts.

  ## Usage

      alias OVSDB.{Schema, SchemaHelper, MonitorSpec}

      {:ok, full} = Schema.parse_string(File.read!("priv/opensync.ovsschema"))

      helper =
        SchemaHelper.new(full)
        |> SchemaHelper.register_columns("AWLAN_Node", ["manager_addr", "redirector_addr"])
        |> SchemaHelper.register_columns("Wifi_VIF_Config", ["if_name", "enabled", "ssid"])
        |> SchemaHelper.register_table("Wifi_VIF_State")  # all columns

      {:ok, filtered} = SchemaHelper.get_idl_schema(helper)
      monitor = SchemaHelper.get_monitor_spec(helper, "sim-1")

  ## Registration semantics

    * `register_table/2` — register interest in a table with **all**
      its columns. Equivalent to passing every column to
      `register_columns/3`.

    * `register_columns/3` — register interest in specific columns.
      Multiple calls accumulate. Safe to call before or after
      `register_table/2`.

  ## Precedence: "all" beats "some"

  If both `register_table/2` and `register_columns/3` are called
  for the same table, the result is **all columns** — promoting a
  subset registration to a full one is useful (you realized you
  need more than you thought); narrowing isn't.

  ## Unknown tables/columns

  `register_table/2` returns `{:error, {:unknown_table, name}}` if
  the table isn't in the source schema. `register_columns/3`
  similarly rejects unknown columns. Both return `{:ok, helper}` on
  success — chain via `with`, or use the bang variants `register_table!/2`
  and `register_columns!/3` if you want to let it crash.
  """

  alias OVSDB.{MonitorSpec, Schema}
  alias OVSDB.Schema.{Column, Table}

  @enforce_keys [:source]
  defstruct source: nil, registrations: %{}

  @typedoc """
  Per-table registration:

    * `:all` — all columns of the table are of interest
    * `{:columns, MapSet.t(String.t())}` — only the named columns
  """
  @type registration :: :all | {:columns, MapSet.t()}

  @type t :: %__MODULE__{
          source: Schema.t(),
          registrations: %{optional(String.t()) => registration()}
        }

  # ---------------------------------------------------------------------------
  # Construction
  # ---------------------------------------------------------------------------

  @doc """
  Creates a new helper wrapping the given full schema. Registrations
  start empty.

      iex> {:ok, schema} = OVSDB.Schema.parse(%{"name" => "db", "tables" => %{}})
      iex> helper = OVSDB.SchemaHelper.new(schema)
      iex> helper.registrations
      %{}
  """
  @spec new(Schema.t()) :: t()
  def new(%Schema{} = source) do
    %__MODULE__{source: source, registrations: %{}}
  end

  # ---------------------------------------------------------------------------
  # Registration — bang variants at the top because the tail functions
  # below all delegate to the validated path.
  # ---------------------------------------------------------------------------

  @doc """
  Registers interest in all columns of a table.

  Returns `{:ok, helper}` if the table exists in the source schema,
  otherwise `{:error, {:unknown_table, name}}`.

  If `register_columns/3` was previously called for this table, this
  call promotes the registration to "all columns."

      iex> {:ok, schema} = OVSDB.Schema.parse(%{
      ...>   "name" => "db",
      ...>   "tables" => %{"T" => %{"columns" => %{"c" => %{"type" => "string"}}}}
      ...> })
      iex> helper = OVSDB.SchemaHelper.new(schema)
      iex> {:ok, helper} = OVSDB.SchemaHelper.register_table(helper, "T")
      iex> helper.registrations
      %{"T" => :all}

      iex> {:ok, schema} = OVSDB.Schema.parse(%{"name" => "db", "tables" => %{}})
      iex> helper = OVSDB.SchemaHelper.new(schema)
      iex> OVSDB.SchemaHelper.register_table(helper, "Missing")
      {:error, {:unknown_table, "Missing"}}
  """
  @spec register_table(t(), String.t()) :: {:ok, t()} | {:error, term()}
  def register_table(%__MODULE__{source: schema, registrations: regs} = helper, table_name)
      when is_binary(table_name) do
    case Schema.table(schema, table_name) do
      :error ->
        {:error, {:unknown_table, table_name}}

      {:ok, _table} ->
        {:ok, %{helper | registrations: Elixir.Map.put(regs, table_name, :all)}}
    end
  end

  @doc """
  Bang variant of `register_table/2`. Raises on unknown table.
  Useful when chaining with `|>`.

      iex> {:ok, schema} = OVSDB.Schema.parse(%{
      ...>   "name" => "db",
      ...>   "tables" => %{"T" => %{"columns" => %{"c" => %{"type" => "string"}}}}
      ...> })
      iex> OVSDB.SchemaHelper.new(schema)
      ...> |> OVSDB.SchemaHelper.register_table!("T")
      ...> |> Map.get(:registrations)
      %{"T" => :all}
  """
  @spec register_table!(t(), String.t()) :: t()
  def register_table!(%__MODULE__{} = helper, table_name) do
    case register_table(helper, table_name) do
      {:ok, updated} -> updated
      {:error, reason} -> raise ArgumentError, "register_table! failed: #{inspect(reason)}"
    end
  end

  @doc """
  Registers interest in specific columns of a table. Multiple calls
  accumulate — calling twice with different column lists means "I
  want the union."

  If the table is already registered as `:all`, this call is a no-op
  (all columns beats any subset).

  Returns `{:error, {:unknown_table, name}}` if the table doesn't
  exist, or `{:error, {:unknown_columns, table, [cols]}}` if any
  column is unknown.

      iex> {:ok, schema} = OVSDB.Schema.parse(%{
      ...>   "name" => "db",
      ...>   "tables" => %{"T" => %{"columns" => %{
      ...>     "a" => %{"type" => "string"},
      ...>     "b" => %{"type" => "integer"},
      ...>     "c" => %{"type" => "boolean"}
      ...>   }}}
      ...> })
      iex> helper = OVSDB.SchemaHelper.new(schema)
      iex> {:ok, helper} = OVSDB.SchemaHelper.register_columns(helper, "T", ["a", "b"])
      iex> {:ok, helper} = OVSDB.SchemaHelper.register_columns(helper, "T", ["b", "c"])
      iex> {:columns, set} = helper.registrations["T"]
      iex> MapSet.to_list(set) |> Enum.sort()
      ["a", "b", "c"]
  """
  @spec register_columns(t(), String.t(), [String.t()]) :: {:ok, t()} | {:error, term()}
  def register_columns(%__MODULE__{source: schema, registrations: regs} = helper, table_name, cols)
      when is_binary(table_name) and is_list(cols) do
    with {:ok, table} <- fetch_table(schema, table_name),
         :ok <- check_columns_exist(table, cols) do
      new_reg =
        case Elixir.Map.get(regs, table_name) do
          :all ->
            # "all" beats "some" — don't narrow
            :all

          {:columns, existing} ->
            {:columns, MapSet.union(existing, MapSet.new(cols))}

          nil ->
            {:columns, MapSet.new(cols)}
        end

      {:ok, %{helper | registrations: Elixir.Map.put(regs, table_name, new_reg)}}
    end
  end

  @doc """
  Bang variant of `register_columns/3`. Raises on any error.
  """
  @spec register_columns!(t(), String.t(), [String.t()]) :: t()
  def register_columns!(%__MODULE__{} = helper, table_name, cols) do
    case register_columns(helper, table_name, cols) do
      {:ok, updated} -> updated
      {:error, reason} -> raise ArgumentError, "register_columns! failed: #{inspect(reason)}"
    end
  end

  # ---------------------------------------------------------------------------
  # Introspection
  # ---------------------------------------------------------------------------

  @doc """
  Returns the sorted list of registered table names.

      iex> {:ok, schema} = OVSDB.Schema.parse(%{
      ...>   "name" => "db",
      ...>   "tables" => %{
      ...>     "A" => %{"columns" => %{"x" => %{"type" => "string"}}},
      ...>     "B" => %{"columns" => %{"x" => %{"type" => "string"}}}
      ...>   }
      ...> })
      iex> helper = OVSDB.SchemaHelper.new(schema)
      iex> {:ok, helper} = OVSDB.SchemaHelper.register_table(helper, "A")
      iex> {:ok, helper} = OVSDB.SchemaHelper.register_table(helper, "B")
      iex> OVSDB.SchemaHelper.registered_tables(helper)
      ["A", "B"]
  """
  @spec registered_tables(t()) :: [String.t()]
  def registered_tables(%__MODULE__{registrations: regs}),
    do: regs |> Elixir.Map.keys() |> Enum.sort()

  @doc """
  Returns the sorted list of registered column names for a table,
  resolving `:all` against the source schema. Returns `[]` for
  unregistered tables.
  """
  @spec registered_columns(t(), String.t()) :: [String.t()]
  def registered_columns(%__MODULE__{source: schema, registrations: regs}, table_name) do
    case Elixir.Map.get(regs, table_name) do
      nil ->
        []

      :all ->
        case Schema.table(schema, table_name) do
          {:ok, table} -> Table.column_names(table)
          :error -> []
        end

      {:columns, set} ->
        set |> MapSet.to_list() |> Enum.sort()
    end
  end

  @doc """
  Returns `true` if no tables have been registered.
  """
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{registrations: regs}), do: map_size(regs) == 0

  # ---------------------------------------------------------------------------
  # Output — the whole point of this module
  # ---------------------------------------------------------------------------

  @doc """
  Produces a filtered `Schema` containing only registered tables,
  and within each table only registered columns.

  This is what the IDL consumes — it's the "working schema" for the
  client, smaller than the full schema the server knows about.

  Returns `{:error, :no_registrations}` if nothing has been
  registered yet (since an empty IDL is almost certainly a bug).

      iex> {:ok, schema} = OVSDB.Schema.parse(%{
      ...>   "name" => "db", "version" => "1.0.0",
      ...>   "tables" => %{
      ...>     "A" => %{"columns" => %{
      ...>       "x" => %{"type" => "string"},
      ...>       "y" => %{"type" => "integer"}
      ...>     }},
      ...>     "B" => %{"columns" => %{"z" => %{"type" => "boolean"}}}
      ...>   }
      ...> })
      iex> helper = OVSDB.SchemaHelper.new(schema)
      iex> {:ok, helper} = OVSDB.SchemaHelper.register_columns(helper, "A", ["x"])
      iex> {:ok, filtered} = OVSDB.SchemaHelper.get_idl_schema(helper)
      iex> filtered.name
      "db"
      iex> OVSDB.Schema.table_names(filtered)
      ["A"]
      iex> {:ok, a_table} = OVSDB.Schema.table(filtered, "A")
      iex> OVSDB.Schema.Table.column_names(a_table)
      ["x"]
  """
  @spec get_idl_schema(t()) :: {:ok, Schema.t()} | {:error, :no_registrations}
  def get_idl_schema(%__MODULE__{registrations: regs}) when map_size(regs) == 0 do
    {:error, :no_registrations}
  end

  def get_idl_schema(%__MODULE__{source: %Schema{} = source, registrations: regs}) do
    filtered_tables =
      for {name, reg} <- regs, into: %{} do
        {:ok, table} = Schema.table(source, name)
        {name, filter_table(table, reg)}
      end

    {:ok, %{source | tables: filtered_tables}}
  end

  @doc """
  Produces an `OVSDB.MonitorSpec` that subscribes to every
  registered table and column. Uses the given `monitor_id` as the
  subscription handle.

  The `select` flags default to RFC-default (all four categories).
  Pass `select:` to override globally for every table.

      iex> {:ok, schema} = OVSDB.Schema.parse(%{
      ...>   "name" => "Open_vSwitch",
      ...>   "tables" => %{"Bridge" => %{"columns" => %{"name" => %{"type" => "string"}}}}
      ...> })
      iex> helper = OVSDB.SchemaHelper.new(schema)
      iex> {:ok, helper} = OVSDB.SchemaHelper.register_columns(helper, "Bridge", ["name"])
      iex> spec = OVSDB.SchemaHelper.get_monitor_spec(helper, "m1")
      iex> spec.db
      "Open_vSwitch"
      iex> spec.monitor_id
      "m1"
      iex> spec.tables
      %{"Bridge" => %{columns: ["name"]}}
  """
  @spec get_monitor_spec(t(), MonitorSpec.monitor_id(), keyword()) :: MonitorSpec.t()
  def get_monitor_spec(%__MODULE__{source: source} = helper, monitor_id, opts \\ []) do
    base = MonitorSpec.new(source.name, monitor_id)
    select = Keyword.get(opts, :select)

    Enum.reduce(helper.registrations, base, fn {table_name, _reg}, spec ->
      cols = registered_columns(helper, table_name)
      watch_opts = if select, do: [columns: cols, select: select], else: [columns: cols]
      MonitorSpec.watch(spec, table_name, watch_opts)
    end)
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp fetch_table(schema, table_name) do
    case Schema.table(schema, table_name) do
      :error -> {:error, {:unknown_table, table_name}}
      ok -> ok
    end
  end

  defp check_columns_exist(%Table{} = table, cols) do
    unknown =
      Enum.reject(cols, fn col ->
        match?({:ok, %Column{}}, Table.column(table, col))
      end)

    case unknown do
      [] -> :ok
      _ -> {:error, {:unknown_columns, table.name, unknown}}
    end
  end

  defp filter_table(%Table{columns: cols} = table, :all), do: %{table | columns: cols}

  defp filter_table(%Table{columns: cols} = table, {:columns, set}) do
    filtered =
      cols
      |> Enum.filter(fn {name, _} -> MapSet.member?(set, name) end)
      |> Elixir.Map.new()

    %{table | columns: filtered}
  end
end
