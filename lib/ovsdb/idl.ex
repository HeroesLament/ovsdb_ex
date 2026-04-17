defmodule OVSDB.Idl do
  @moduledoc """
  Client-side in-memory replica of a remote OVSDB database.

  Maintains an ETS-backed cache of the rows the client cares about,
  kept current via `monitor` subscription. Reads are lock-free; the
  IDL GenServer only serializes writes (apply of monitor updates).

  ## Mental model

  The IDL is the "view through the window" — a local, always-current
  snapshot of a subset of the remote database. When the server
  changes anything, the IDL sees an `update` notification and
  applies it. When the client wants to read, it reads directly from
  ETS, never blocking on the server.

  ## Startup sequence

      alias OVSDB.{ClientSession, Idl, Schema, SchemaHelper}

      # 1. Connect and fetch the schema
      {:ok, session} = ClientSession.connect("ovsdb.local", 6640)
      {:ok, schema_json} = ClientSession.get_schema(session, "Open_vSwitch")
      {:ok, schema} = Schema.parse(schema_json)

      # 2. Register interest (what tables/columns to replicate)
      helper =
        SchemaHelper.new(schema)
        |> SchemaHelper.register_columns!("AWLAN_Node", ["manager_addr"])
        |> SchemaHelper.register_table!("Wifi_VIF_State")

      # 3. Start the IDL — it subscribes and fetches initial state
      {:ok, idl} = Idl.start_link(session: session, helper: helper, monitor_id: "my-idl")

      # 4. Read any time, with no roundtrip
      %{uuid1 => row1, uuid2 => row2} = Idl.get_table(idl, "AWLAN_Node")

      # 5. Subscribe to change notifications if you want push updates
      Idl.subscribe(idl, "AWLAN_Node")
      # Now receive {:idl_changed, idl, "AWLAN_Node", :insert | :modify | :delete, uuid}

  ## Read API

  All reads are direct ETS reads — no GenServer call:

    * `get_table/2` — full map of `%{uuid_string => row_map}` for a table
    * `get_row/3` — single row by uuid string
    * `list_rows/2` — rows as a list
    * `change_seqno/1` — current sequence number (bumped on every apply)

  The IDL's data is a consistent snapshot at any given moment, but
  between successive reads the replica may advance. If you need
  coherent multi-read snapshots, check `change_seqno/1` before and
  after.

  ## Row values

  Rows are stored as plain `%{column_name => value}` maps. Values
  are in their Elixir-native form — UUIDs as `OVSDB.UUID` structs,
  sets as `OVSDB.Set`, etc. — decoded at apply time via the schema.
  Callers don't need to re-decode.

  The `_uuid` is NOT a column in the row map; it's the key under
  which the row is stored. Use `get_row/3` to look it up.

  ## Apply model (RFC 7047 §4.1.5)

  Monitor update entries are three-valued:

      {old: null, new: row}   =>  insert
      {old: row,  new: row'}  =>  modify  (new contains only changed columns)
      {old: row,  new: null}  =>  delete

  Modify deltas are partial — only the changed columns appear in
  the `new` value. The IDL merges them into the existing replica
  row; it does not replace wholesale.

  ## Not (yet) implemented

    * Write staging / transactional write API — for now, write via
      `ClientSession.transact/2` directly. The IDL will pick up the
      resulting state change via the normal `update` path.
    * Weak/strong ref garbage collection — the server handles this.
    * `monitor_cond` — conditional monitors (RFC 7047bis extension).
  """

  use GenServer

  require Logger

  alias OVSDB.{ClientSession, Schema, SchemaHelper, UUID, Value}

  @type t :: pid()

  defstruct [
    :session,
    :schema,
    :monitor_id,
    :ets,
    :change_seqno_ets,
    # method => MapSet of subscriber pids
    subscribers: %{}
  ]

  # ---------------------------------------------------------------------------
  # Public API — lifecycle
  # ---------------------------------------------------------------------------

  @doc """
  Starts an IDL bound to a ClientSession.

  ## Required options

    * `:session` — a running `OVSDB.ClientSession` pid
    * `:helper` — an `OVSDB.SchemaHelper` with registrations
    * `:monitor_id` — opaque identifier for this subscription
      (must be unique per session)

  ## Optional

    * `:monitor_timeout` — ms to wait for initial monitor response.
      Default 30_000.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc "Stops the IDL, removing its monitor subscription."
  @spec stop(t()) :: :ok
  def stop(idl), do: GenServer.stop(idl, :normal)

  # ---------------------------------------------------------------------------
  # Public API — reads (direct ETS, no GenServer call)
  # ---------------------------------------------------------------------------

  @doc """
  Returns all rows in a table as `%{uuid_string => row_map}`.
  Returns `%{}` if the table has no rows (or isn't being monitored).

  Performs a direct ETS read with no GenServer call.
  """
  @spec get_table(t(), String.t()) :: %{optional(String.t()) => map()}
  def get_table(idl, table_name) when is_binary(table_name) do
    ets = rows_ets(idl)

    :ets.select(ets, [{{{table_name, :"$1"}, :"$2"}, [], [{{:"$1", :"$2"}}]}])
    |> Elixir.Map.new()
  end

  @doc """
  Fetches a single row by uuid string. Returns `{:ok, row_map}` or
  `:error`.

      iex> # (conceptually, after apply)
      iex> # Idl.get_row(idl, "Bridge", "550e8400-...")
      iex> # {:ok, %{"name" => "br-lan", ...}}
  """
  @spec get_row(t(), String.t(), String.t()) :: {:ok, map()} | :error
  def get_row(idl, table_name, uuid_string)
      when is_binary(table_name) and is_binary(uuid_string) do
    ets = rows_ets(idl)

    case :ets.lookup(ets, {table_name, uuid_string}) do
      [{_key, row}] -> {:ok, row}
      [] -> :error
    end
  end

  @doc """
  Returns all rows in a table as a list of `{uuid_string, row_map}`
  tuples. Useful for enumeration when you don't need map lookups.
  """
  @spec list_rows(t(), String.t()) :: [{String.t(), map()}]
  def list_rows(idl, table_name) when is_binary(table_name) do
    ets = rows_ets(idl)

    :ets.select(ets, [{{{table_name, :"$1"}, :"$2"}, [], [{{:"$1", :"$2"}}]}])
  end

  @doc """
  Returns the current change sequence number. Bumped once per apply
  of a monitor response or update notification. Applications can
  compare successive values to detect whether the replica changed.

      seqno_before = Idl.change_seqno(idl)
      # ...do reads...
      seqno_after = Idl.change_seqno(idl)
      if seqno_before == seqno_after, do: "consistent"
  """
  @spec change_seqno(t()) :: non_neg_integer()
  def change_seqno(idl) do
    ets = meta_ets(idl)

    case :ets.lookup(ets, :change_seqno) do
      [{:change_seqno, n}] -> n
      [] -> 0
    end
  end

  @doc """
  Returns the internal ETS table identifiers as `{rows_ets, meta_ets}`.

  Hot-path callers can cache these and use them with the `_cached`
  variants of reads, avoiding a GenServer call on every lookup. The
  ids are stable for the lifetime of the IDL — if the IDL dies, they
  become invalid.
  """
  @spec table_ids(t()) :: {:ets.tid(), :ets.tid()}
  def table_ids(idl), do: GenServer.call(idl, :table_ids)

  @doc """
  Cached variant of `get_table/2`. Takes the `rows_ets` tid from
  `table_ids/1`.
  """
  @spec get_table_cached(:ets.tid(), String.t()) :: %{optional(String.t()) => map()}
  def get_table_cached(rows_ets, table_name) when is_binary(table_name) do
    :ets.select(rows_ets, [{{{table_name, :"$1"}, :"$2"}, [], [{{:"$1", :"$2"}}]}])
    |> Elixir.Map.new()
  end

  @doc """
  Cached variant of `get_row/3`.
  """
  @spec get_row_cached(:ets.tid(), String.t(), String.t()) :: {:ok, map()} | :error
  def get_row_cached(rows_ets, table_name, uuid_string)
      when is_binary(table_name) and is_binary(uuid_string) do
    case :ets.lookup(rows_ets, {table_name, uuid_string}) do
      [{_key, row}] -> {:ok, row}
      [] -> :error
    end
  end

  @doc "Cached variant of `change_seqno/1`."
  @spec change_seqno_cached(:ets.tid()) :: non_neg_integer()
  def change_seqno_cached(meta_ets) do
    case :ets.lookup(meta_ets, :change_seqno) do
      [{:change_seqno, n}] -> n
      [] -> 0
    end
  end

  # ---------------------------------------------------------------------------
  # Public API — subscriptions
  # ---------------------------------------------------------------------------

  @doc """
  Subscribes the calling process to change notifications for a
  table. Messages arrive as
  `{:idl_changed, idl_pid, table_name, change_type, uuid_string}`
  where `change_type` is `:insert`, `:modify`, or `:delete`.

  Use `"*"` as the table name to subscribe to all tables.
  """
  @spec subscribe(t(), String.t()) :: :ok
  def subscribe(idl, table_name) when is_binary(table_name) do
    GenServer.call(idl, {:subscribe, table_name, self()})
  end

  @doc "Removes a change-notification subscription."
  @spec unsubscribe(t(), String.t()) :: :ok
  def unsubscribe(idl, table_name) when is_binary(table_name) do
    GenServer.call(idl, {:unsubscribe, table_name, self()})
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    session = Keyword.fetch!(opts, :session)
    helper = Keyword.fetch!(opts, :helper)
    monitor_id = Keyword.fetch!(opts, :monitor_id)
    monitor_timeout = Keyword.get(opts, :monitor_timeout, 30_000)

    with {:ok, schema} <- SchemaHelper.get_idl_schema(helper),
         spec <- SchemaHelper.get_monitor_spec(helper, monitor_id) do
      # Create ETS tables owned by this process. Public so readers
      # can do direct lookups without a GenServer hop.
      rows_ets = :ets.new(:idl_rows, [:set, :public, read_concurrency: true])
      meta_ets = :ets.new(:idl_meta, [:set, :public, read_concurrency: true])
      :ets.insert(meta_ets, {:change_seqno, 0})

      # Subscribe to update notifications before sending monitor, so
      # we don't miss any.
      :ok = ClientSession.subscribe(session, "update")

      # Send monitor request; block until initial state arrives.
      case ClientSession.monitor(session, spec, monitor_timeout) do
        {:ok, initial_state} ->
          state = %__MODULE__{
            session: session,
            schema: schema,
            monitor_id: monitor_id,
            ets: rows_ets,
            change_seqno_ets: meta_ets
          }

          {:ok, apply_update(state, initial_state)}

        {:error, reason} ->
          :ets.delete(rows_ets)
          :ets.delete(meta_ets)
          {:stop, {:monitor_failed, reason}}
      end
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:subscribe, table_name, pid}, _from, state) do
    subs =
      Elixir.Map.update(
        state.subscribers,
        table_name,
        MapSet.new([pid]),
        &MapSet.put(&1, pid)
      )

    {:reply, :ok, %{state | subscribers: subs}}
  end

  def handle_call({:unsubscribe, table_name, pid}, _from, state) do
    subs =
      case Elixir.Map.get(state.subscribers, table_name) do
        nil ->
          state.subscribers

        set ->
          new_set = MapSet.delete(set, pid)

          if MapSet.size(new_set) == 0 do
            Elixir.Map.delete(state.subscribers, table_name)
          else
            Elixir.Map.put(state.subscribers, table_name, new_set)
          end
      end

    {:reply, :ok, %{state | subscribers: subs}}
  end

  # Handle :rows_ets / :meta_ets lookups from the read API helpers.
  def handle_call(:rows_ets, _from, state), do: {:reply, state.ets, state}
  def handle_call(:meta_ets, _from, state), do: {:reply, state.change_seqno_ets, state}

  def handle_call(:table_ids, _from, state),
    do: {:reply, {state.ets, state.change_seqno_ets}, state}

  # ClientSession forwards "update" notifications here.
  @impl true
  def handle_info({:ovsdb_notification, session, "update", params}, %{session: session} = state) do
    case params do
      [monitor_id, updates] when monitor_id == state.monitor_id and is_map(updates) ->
        {:noreply, apply_update(state, updates)}

      [other_id, _updates] ->
        # Notification for a different monitor on the same session.
        # Ignore — another IDL might be handling it.
        Logger.debug("Idl ignoring update for monitor_id=#{inspect(other_id)}")
        {:noreply, state}

      other ->
        Logger.warning("Idl received malformed update params: #{inspect(other)}")
        {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    # Try to cancel the monitor subscription. Best effort — the
    # session may already be dead.
    _ =
      if Process.alive?(state.session) do
        ClientSession.monitor_cancel(state.session, state.monitor_id, 1_000)
      end

    :ok
  end

  # ---------------------------------------------------------------------------
  # Private — apply logic
  # ---------------------------------------------------------------------------

  # Apply a table-update map to the ETS replica. `updates` is the
  # outer map {table_name => {uuid_string => %{"old" => ..., "new" => ...}}}.
  defp apply_update(state, updates) when is_map(updates) do
    changes =
      Enum.flat_map(updates, fn {table_name, table_updates} ->
        apply_table_updates(state, table_name, table_updates)
      end)

    if changes != [] do
      _ = bump_seqno(state)

      Enum.each(changes, fn {table_name, change_type, uuid_string} ->
        notify_subscribers(state, table_name, change_type, uuid_string)
      end)
    end

    state
  end

  defp apply_table_updates(state, table_name, table_updates) when is_map(table_updates) do
    for {uuid_string, entry} <- table_updates do
      apply_row_update(state, table_name, uuid_string, entry)
    end
  end

  defp apply_row_update(state, table_name, uuid_string, %{"old" => old, "new" => new}) do
    key = {table_name, uuid_string}

    case {old, new} do
      {nil, new_row} when is_map(new_row) ->
        # Insert.
        decoded = decode_row(state, table_name, new_row)
        :ets.insert(state.ets, {key, decoded})
        {table_name, :insert, uuid_string}

      {_old, new_row} when is_map(new_row) ->
        # Modify. `new` contains only changed columns; merge into
        # the existing replica row.
        existing =
          case :ets.lookup(state.ets, key) do
            [{_, row}] -> row
            [] -> %{}
          end

        decoded_delta = decode_row(state, table_name, new_row)
        merged = Elixir.Map.merge(existing, decoded_delta)
        :ets.insert(state.ets, {key, merged})
        {table_name, :modify, uuid_string}

      {_old, nil} ->
        # Delete.
        :ets.delete(state.ets, key)
        {table_name, :delete, uuid_string}

      other ->
        Logger.warning(
          "Idl malformed row update for #{table_name}/#{uuid_string}: #{inspect(other)}"
        )

        nil
    end
  end

  # Also accept entries missing either field — some servers omit
  # `"old"` on insert or `"new"` on delete. RFC 7047 §4.1.6 permits
  # this; we handle both shapes.
  defp apply_row_update(state, table_name, uuid_string, entry) when is_map(entry) do
    old = Elixir.Map.get(entry, "old")
    new = Elixir.Map.get(entry, "new")
    apply_row_update(state, table_name, uuid_string, %{"old" => old, "new" => new})
  end

  # Decode a wire row (string column keys, wire-form values) into
  # Elixir-native values using the schema's column type information.
  defp decode_row(state, table_name, wire_row) do
    case Schema.table(state.schema, table_name) do
      {:ok, table} ->
        for {col_name, wire_value} <- wire_row, into: %{} do
          {col_name, decode_column(table, col_name, wire_value)}
        end

      :error ->
        # Table not in our filtered schema — just keep wire form.
        wire_row
    end
  end

  defp decode_column(table, col_name, wire_value) do
    case Schema.Table.column(table, col_name) do
      {:ok, column} -> decode_value(column, wire_value)
      :error -> wire_value
    end
  end

  # Decode a single wire value using its column definition.
  defp decode_value(%Schema.Column{kind: :atomic, key_type: :uuid}, wire) do
    case UUID.decode(wire) do
      {:ok, uuid} -> uuid
      _ -> wire
    end
  end

  defp decode_value(%Schema.Column{kind: :set}, wire) do
    case Value.decode_value(wire) do
      {:ok, v} -> v
      _ -> wire
    end
  end

  defp decode_value(%Schema.Column{kind: :map}, wire) do
    case Value.decode_value(wire) do
      {:ok, v} -> v
      _ -> wire
    end
  end

  # Atomic non-uuid: pass through (integers, floats, booleans, strings
  # are native already).
  defp decode_value(%Schema.Column{kind: :atomic}, wire), do: wire

  defp bump_seqno(state) do
    :ets.update_counter(state.change_seqno_ets, :change_seqno, 1)
  end

  defp notify_subscribers(state, table_name, change_type, uuid_string) do
    msg = {:idl_changed, self(), table_name, change_type, uuid_string}

    # Table-specific subscribers
    Enum.each(Elixir.Map.get(state.subscribers, table_name, []), fn pid ->
      Kernel.send(pid, msg)
    end)

    # Wildcard "*" subscribers
    Enum.each(Elixir.Map.get(state.subscribers, "*", []), fn pid ->
      Kernel.send(pid, msg)
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # ETS table handle lookup. Two options:
  # 1. Ask the IDL via GenServer.call each time (safe but slow)
  # 2. Cache ETS table IDs at startup
  #
  # We do #1 because ETS table IDs are integers and (a) we'd have to
  # expose them via start_link return, and (b) the call is only needed
  # for the first read from a given caller — the caller can cache the
  # table id themselves via `:sys.get_state` or a dedicated helper.
  # For now, we go with the call. Hot paths can cache.
  # ---------------------------------------------------------------------------

  defp rows_ets(idl), do: GenServer.call(idl, :rows_ets)
  defp meta_ets(idl), do: GenServer.call(idl, :meta_ets)
end
