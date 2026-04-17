defmodule OVSDB.ClientSessionTest do
  use ExUnit.Case, async: true

  alias OVSDB.{ClientSession, MonitorSpec, Operation, Protocol, Transaction, Transport}

  # Set up a client session talking to a fake server Transport that
  # we drive by hand. Both ends live in the test process.
  defp make_pair do
    {:ok, lsock} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])
    {:ok, port} = :inet.port(lsock)

    parent = self()

    spawn_link(fn ->
      {:ok, sock} = :gen_tcp.accept(lsock, 5_000)
      :ok = :gen_tcp.controlling_process(sock, parent)
      Kernel.send(parent, {:server_socket, sock})
    end)

    {:ok, session} = ClientSession.connect("127.0.0.1", port)

    server_sock =
      receive do
        {:server_socket, s} -> s
      after
        2_000 -> flunk("server socket not received")
      end

    {:ok, server} = Transport.wrap(server_sock)

    on_exit = fn ->
      safe_stop(session, &ClientSession.close/1)
      safe_stop(server, &Transport.close/1)
      _ = :gen_tcp.close(lsock)
    end

    {session, server, on_exit}
  end

  # Tolerates the race where a pid dies between our Process.alive?
  # check and our stop call. That happens in these tests because
  # closing the client side often triggers the server side to close
  # too, via socket close propagation.
  defp safe_stop(pid, stop_fn) do
    if Process.alive?(pid) do
      try do
        stop_fn.(pid)
      catch
        :exit, _ -> :ok
      end
    end
  end

  # Drive the fake server: wait for a request, respond via Transport.send.
  defp respond_once(server, result) do
    receive do
      {:ovsdb_message, ^server, %{"id" => id} = _req} ->
        Transport.send(server, Protocol.response(id, result))
    after
      2_000 -> flunk("no request seen at server")
    end
  end

  describe "list_dbs/2" do
    test "round-trips through the wire" do
      {session, server, on_exit} = make_pair()
      on_exit(on_exit)

      task = Task.async(fn -> ClientSession.list_dbs(session) end)
      respond_once(server, ["Open_vSwitch"])
      assert {:ok, ["Open_vSwitch"]} = Task.await(task, 2_000)
    end
  end

  describe "get_schema/3" do
    test "sends db name and returns schema JSON" do
      {session, server, on_exit} = make_pair()
      on_exit(on_exit)

      schema_json = %{"name" => "db", "tables" => %{}}

      task = Task.async(fn -> ClientSession.get_schema(session, "db") end)

      receive do
        {:ovsdb_message, ^server, %{"id" => id, "method" => "get_schema", "params" => ["db"]}} ->
          Transport.send(server, Protocol.response(id, schema_json))
      after
        2_000 -> flunk("no get_schema seen")
      end

      assert {:ok, ^schema_json} = Task.await(task, 2_000)
    end
  end

  describe "transact/3" do
    test "sends transaction and returns results" do
      {session, server, on_exit} = make_pair()
      on_exit(on_exit)

      txn =
        Transaction.new("db")
        |> Transaction.add(Operation.insert("T", %{"x" => 1}))

      task = Task.async(fn -> ClientSession.transact(session, txn) end)

      receive do
        {:ovsdb_message, ^server, %{"id" => id, "method" => "transact"}} ->
          Transport.send(server, Protocol.response(id, [%{"count" => 1}]))
      after
        2_000 -> flunk("no transact seen")
      end

      assert {:ok, [%{"count" => 1}]} = Task.await(task, 2_000)
    end
  end

  describe "monitor/3" do
    test "returns initial state map" do
      {session, server, on_exit} = make_pair()
      on_exit(on_exit)

      spec = MonitorSpec.new("db", "m1") |> MonitorSpec.watch("T")

      task = Task.async(fn -> ClientSession.monitor(session, spec) end)

      receive do
        {:ovsdb_message, ^server, %{"id" => id, "method" => "monitor"}} ->
          Transport.send(server, Protocol.response(id, %{"T" => %{}}))
      after
        2_000 -> flunk("no monitor seen")
      end

      assert {:ok, %{"T" => %{}}} = Task.await(task, 2_000)
    end
  end

  describe "error response path" do
    test "server error is surfaced as {:error, {:ovsdb, reason}}" do
      {session, server, on_exit} = make_pair()
      on_exit(on_exit)

      task = Task.async(fn -> ClientSession.list_dbs(session) end)

      receive do
        {:ovsdb_message, ^server, %{"id" => id}} ->
          Transport.send(server, Protocol.error_response(id, "not supported"))
      after
        2_000 -> flunk("no request seen")
      end

      assert {:error, {:ovsdb, "not supported"}} = Task.await(task, 2_000)
    end
  end

  describe "request ID correlation" do
    test "concurrent requests receive their own responses" do
      {session, server, on_exit} = make_pair()
      on_exit(on_exit)

      t1 = Task.async(fn -> ClientSession.echo(session, ["a"]) end)
      t2 = Task.async(fn -> ClientSession.echo(session, ["b"]) end)

      # Collect both requests at the server, note their ids
      req1 =
        receive do
          {:ovsdb_message, ^server, %{"id" => id, "params" => ["a"]}} -> id
        after
          2_000 -> flunk("no req1")
        end

      req2 =
        receive do
          {:ovsdb_message, ^server, %{"id" => id, "params" => ["b"]}} -> id
        after
          2_000 -> flunk("no req2")
        end

      # Reply out of order: respond to req2 first
      Transport.send(server, Protocol.response(req2, ["b"]))
      Transport.send(server, Protocol.response(req1, ["a"]))

      # Each task should get its own answer regardless of reply order
      assert {:ok, ["a"]} = Task.await(t1, 2_000)
      assert {:ok, ["b"]} = Task.await(t2, 2_000)
    end
  end

  describe "timeout" do
    test "returns :timeout if server doesn't respond within the timeout" do
      {session, _server, on_exit} = make_pair()
      on_exit(on_exit)

      # Don't respond. Request should time out after 100ms.
      assert {:error, :timeout} = ClientSession.list_dbs(session, 100)
    end
  end

  describe "notifications" do
    test "subscribed processes receive :ovsdb_notification" do
      {session, server, on_exit} = make_pair()
      on_exit(on_exit)

      :ok = ClientSession.subscribe(session, "update")

      note = Protocol.notification("update", ["monitor-1", %{"T" => %{}}])
      Transport.send(server, note)

      assert_receive {:ovsdb_notification, ^session, "update",
                      ["monitor-1", %{"T" => %{}}]},
                     1_000
    end

    test "only subscribers of that method receive notifications" do
      {session, server, on_exit} = make_pair()
      on_exit(on_exit)

      :ok = ClientSession.subscribe(session, "locked")

      # Send an unrelated notification
      note = Protocol.notification("update", ["m", %{}])
      Transport.send(server, note)

      # We should NOT receive it
      refute_receive {:ovsdb_notification, ^session, "update", _}, 200
    end

    test "unsubscribe stops further delivery" do
      {session, server, on_exit} = make_pair()
      on_exit(on_exit)

      :ok = ClientSession.subscribe(session, "update")
      :ok = ClientSession.unsubscribe(session, "update")

      note = Protocol.notification("update", ["m", %{}])
      Transport.send(server, note)

      refute_receive {:ovsdb_notification, _, _, _}, 200
    end
  end

  describe "close behavior" do
    test "fails pending requests with :closed when socket closes" do
      {session, server, on_exit} = make_pair()
      on_exit(on_exit)

      task = Task.async(fn -> ClientSession.list_dbs(session) end)

      # Wait for request to arrive at server, then close server side
      receive do
        {:ovsdb_message, ^server, _} -> Transport.close(server)
      after
        2_000 -> flunk("no request seen")
      end

      assert {:error, :closed} = Task.await(task, 2_000)
    end
  end
end
