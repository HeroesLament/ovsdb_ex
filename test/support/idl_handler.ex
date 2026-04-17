defmodule OVSDB.TestSupport.IdlHandler do
  @moduledoc """
  Handler that maintains a per-connection "Test_Bridge" table and
  emits update notifications on transact. Used to drive the IDL
  through real-wire integration tests.
  """

  @behaviour OVSDB.ServerSession.Handler

  @schema %{
    "name" => "Test",
    "version" => "1.0.0",
    "tables" => %{
      "Test_Bridge" => %{
        "columns" => %{
          "name" => %{"type" => "string"},
          "count" => %{"type" => "integer"}
        }
      }
    }
  }

  @impl true
  def init(_opts), do: {:ok, %{rows: %{}, monitors: []}}

  @impl true
  def handle_list_dbs(state), do: {:ok, ["Test"], state}

  @impl true
  def handle_get_schema("Test", state), do: {:ok, @schema, state}

  def handle_get_schema(db, state),
    do: {:error, "unknown database: #{db}", state}

  @impl true
  def handle_monitor("Test", monitor_id, _requests, state) do
    initial =
      if map_size(state.rows) == 0 do
        %{}
      else
        rows_json =
          for {uuid, cols} <- state.rows, into: %{} do
            {uuid, %{"new" => cols}}
          end

        %{"Test_Bridge" => rows_json}
      end

    new_monitors = [{monitor_id, ["Test_Bridge"]} | state.monitors]
    {:ok, initial, %{state | monitors: new_monitors}}
  end

  @impl true
  def handle_transact("Test", ops, state) do
    {results, new_rows, deltas} =
      Enum.reduce(ops, {[], state.rows, %{}}, fn op, {results, rows, deltas} ->
        apply_op(op, rows, deltas, results)
      end)

    new_state = %{state | rows: new_rows}

    if deltas != %{} do
      session_pid = self()

      Enum.each(new_state.monitors, fn {mid, _} ->
        OVSDB.ServerSession.notify(session_pid, "update", [mid, deltas])
      end)
    end

    {:ok, Enum.reverse(results), new_state}
  end

  @impl true
  def handle_echo(args, state), do: {:ok, args, state}

  defp apply_op(%{"op" => "insert", "table" => "Test_Bridge", "row" => row}, rows, deltas, results) do
    uuid =
      case Elixir.Map.get(row, "_uuid") do
        nil -> gen_uuid()
        ["uuid", s] -> s
        s when is_binary(s) -> s
      end

    cols = Elixir.Map.delete(row, "_uuid")
    new_rows = Elixir.Map.put(rows, uuid, cols)
    new_deltas = put_in_delta(deltas, uuid, %{"new" => cols, "old" => nil})
    {[%{"uuid" => ["uuid", uuid]} | results], new_rows, new_deltas}
  end

  defp apply_op(
         %{"op" => "update", "table" => "Test_Bridge", "where" => where, "row" => new_cols},
         rows,
         deltas,
         results
       ) do
    matched = matching_uuids(where, rows)

    {new_rows, new_deltas} =
      Enum.reduce(matched, {rows, deltas}, fn uuid, {r, d} ->
        old = Elixir.Map.fetch!(r, uuid)
        merged = Elixir.Map.merge(old, new_cols)
        r2 = Elixir.Map.put(r, uuid, merged)
        d2 = put_in_delta(d, uuid, %{"new" => new_cols, "old" => old})
        {r2, d2}
      end)

    {[%{"count" => length(matched)} | results], new_rows, new_deltas}
  end

  defp apply_op(
         %{"op" => "delete", "table" => "Test_Bridge", "where" => where},
         rows,
         deltas,
         results
       ) do
    matched = matching_uuids(where, rows)

    {new_rows, new_deltas} =
      Enum.reduce(matched, {rows, deltas}, fn uuid, {r, d} ->
        old = Elixir.Map.fetch!(r, uuid)
        r2 = Elixir.Map.delete(r, uuid)
        d2 = put_in_delta(d, uuid, %{"new" => nil, "old" => old})
        {r2, d2}
      end)

    {[%{"count" => length(matched)} | results], new_rows, new_deltas}
  end

  defp apply_op(_op, rows, deltas, results), do: {[%{} | results], rows, deltas}

  defp put_in_delta(deltas, uuid, entry) do
    Elixir.Map.update(
      deltas,
      "Test_Bridge",
      %{uuid => entry},
      &Elixir.Map.put(&1, uuid, entry)
    )
  end

  defp matching_uuids(where, rows) do
    if where == [] do
      Elixir.Map.keys(rows)
    else
      where
      |> Enum.flat_map(&match_clause(&1, rows))
      |> Enum.uniq()
    end
  end

  defp match_clause(["_uuid", "==", ["uuid", u]], rows) do
    if Elixir.Map.has_key?(rows, u), do: [u], else: []
  end

  defp match_clause(["name", "==", n], rows) do
    for {u, c} <- rows, c["name"] == n, do: u
  end

  defp match_clause(_other, _rows), do: []

  defp gen_uuid do
    <<a::32, b::16, c::16, d::16, e::48>> = :crypto.strong_rand_bytes(16)
    c2 = Bitwise.bor(Bitwise.band(c, 0x0FFF), 0x4000)
    d2 = Bitwise.bor(Bitwise.band(d, 0x3FFF), 0x8000)

    :io_lib.format(
      ~c"~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
      [a, b, c2, d2, e]
    )
    |> IO.iodata_to_binary()
  end
end
