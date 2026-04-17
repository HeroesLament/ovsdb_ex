defmodule OVSDB.TestSupport.EchoHandler do
  @moduledoc """
  Minimal handler for tests. Tracks echo calls and
  responds to the core RFC 7047 methods with hardcoded values.
  """

  @behaviour OVSDB.ServerSession.Handler

  @impl true
  def init(opts) do
    state = %{
      dbs: Keyword.get(opts, :dbs, ["Open_vSwitch"]),
      schemas: Keyword.get(opts, :schemas, %{}),
      last_echo: nil
    }

    {:ok, state}
  end

  @impl true
  def handle_list_dbs(state), do: {:ok, state.dbs, state}

  @impl true
  def handle_get_schema(db, state) do
    case Elixir.Map.fetch(state.schemas, db) do
      {:ok, schema} -> {:ok, schema, state}
      :error -> {:error, "unknown database: #{db}", state}
    end
  end

  @impl true
  def handle_echo(args, state) do
    {:ok, args, %{state | last_echo: args}}
  end

  @impl true
  def handle_transact(_db, _ops, state) do
    {:ok, [%{"count" => 0}], state}
  end
end
