defmodule OVSDB.ServerTest do
  use ExUnit.Case, async: true

  alias OVSDB.{ClientSession, Server}
  alias OVSDB.TestSupport.EchoHandler

  # Each test gets its own server on an ephemeral port.

  setup do
    {:ok, srv} = Server.start_link(port: 0, handler: EchoHandler)
    port = Server.listen_port(srv)

    on_exit(fn ->
      if Process.alive?(srv), do: Server.stop(srv)
    end)

    %{server: srv, port: port}
  end

  describe "start_link/1" do
    test "binds to an ephemeral port when port: 0", %{port: port} do
      assert is_integer(port)
      assert port > 0
    end

    test "listen_port/1 returns the bound port", %{server: srv, port: port} do
      assert Server.listen_port(srv) == port
    end
  end

  describe "full request/response round trip" do
    test "list_dbs", %{port: port} do
      {:ok, client} = ClientSession.connect("127.0.0.1", port)
      assert {:ok, ["Open_vSwitch"]} = ClientSession.list_dbs(client)
      ClientSession.close(client)
    end

    test "echo returns args unchanged", %{port: port} do
      {:ok, client} = ClientSession.connect("127.0.0.1", port)
      assert {:ok, ["ping"]} = ClientSession.echo(client, ["ping"])
      ClientSession.close(client)
    end

    test "transact returns the hardcoded echo handler result", %{port: port} do
      alias OVSDB.{Operation, Transaction}

      {:ok, client} = ClientSession.connect("127.0.0.1", port)

      txn =
        Transaction.new("Open_vSwitch")
        |> Transaction.add(Operation.insert("T", %{"x" => 1}))

      assert {:ok, [%{"count" => 0}]} = ClientSession.transact(client, txn)
      ClientSession.close(client)
    end

    test "unknown methods produce 'not supported' error", %{port: port} do
      {:ok, client} = ClientSession.connect("127.0.0.1", port)

      # Our handler doesn't implement monitor, so this should come back
      # as an OVSDB-level error.
      alias OVSDB.MonitorSpec
      spec = MonitorSpec.new("Open_vSwitch", "m1") |> MonitorSpec.watch("T")
      assert {:error, {:ovsdb, "not supported"}} = ClientSession.monitor(client, spec)
      ClientSession.close(client)
    end
  end

  describe "multiple concurrent clients" do
    test "each client gets its own session", %{port: port} do
      {:ok, c1} = ClientSession.connect("127.0.0.1", port)
      {:ok, c2} = ClientSession.connect("127.0.0.1", port)
      {:ok, c3} = ClientSession.connect("127.0.0.1", port)

      tasks = [
        Task.async(fn -> ClientSession.echo(c1, ["a"]) end),
        Task.async(fn -> ClientSession.echo(c2, ["b"]) end),
        Task.async(fn -> ClientSession.echo(c3, ["c"]) end)
      ]

      results = Enum.map(tasks, &Task.await(&1, 2_000))
      assert {:ok, ["a"]} in results
      assert {:ok, ["b"]} in results
      assert {:ok, ["c"]} in results

      ClientSession.close(c1)
      ClientSession.close(c2)
      ClientSession.close(c3)
    end

    test "sessions/1 lists active connection pids", %{server: srv, port: port} do
      {:ok, c1} = ClientSession.connect("127.0.0.1", port)
      {:ok, c2} = ClientSession.connect("127.0.0.1", port)

      # Give the server a moment to register both sessions
      Process.sleep(100)

      sessions = Server.sessions(srv)
      assert length(sessions) == 2

      ClientSession.close(c1)
      ClientSession.close(c2)
    end
  end

  describe "handler error path" do
    test "handler {:error, msg} is surfaced as OVSDB error", %{port: port} do
      {:ok, client} = ClientSession.connect("127.0.0.1", port)

      # Our handler returns an error for unknown databases
      assert {:error, {:ovsdb, msg}} = ClientSession.get_schema(client, "UnknownDB")
      assert is_binary(msg)
      assert msg =~ "unknown database"

      ClientSession.close(client)
    end
  end
end
