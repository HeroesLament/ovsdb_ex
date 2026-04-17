defmodule OVSDB.TransportTest do
  use ExUnit.Case, async: true

  alias OVSDB.{Protocol, Transport}

  # Helper: set up a loopback pair — a client Transport and a server
  # Transport on opposite ends of an accepted TCP socket. Both are
  # owned by the test process, so both deliver their :ovsdb_message
  # notifications to us.
  defp make_pair do
    {:ok, lsock} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])
    {:ok, port} = :inet.port(lsock)

    parent = self()

    spawn_link(fn ->
      {:ok, sock} = :gen_tcp.accept(lsock, 5_000)
      :ok = :gen_tcp.controlling_process(sock, parent)
      Kernel.send(parent, {:server_socket, sock})
    end)

    {:ok, client} = Transport.connect("127.0.0.1", port)

    server_sock =
      receive do
        {:server_socket, s} -> s
      after
        2_000 -> flunk("timed out waiting for server accept")
      end

    {:ok, server} = Transport.wrap(server_sock)

    on_exit = fn ->
      safe_stop(client)
      safe_stop(server)
      _ = :gen_tcp.close(lsock)
    end

    {client, server, on_exit}
  end

  defp safe_stop(pid) do
    if Process.alive?(pid) do
      try do
        Transport.close(pid)
      catch
        :exit, _ -> :ok
      end
    end
  end

  describe "connect/3 + wrap/2 — round trip" do
    test "client can send a message that server receives as ovsdb_message" do
      {client, server, on_exit} = make_pair()
      on_exit(on_exit)

      msg = Protocol.request("list_dbs", [], 1)
      :ok = Transport.send(client, msg)

      assert_receive {:ovsdb_message, ^server, %{"method" => "list_dbs", "id" => 1}}, 1_000
    end

    test "server can send a message that client receives" do
      {client, server, on_exit} = make_pair()
      on_exit(on_exit)

      resp = Protocol.response(1, ["Open_vSwitch"])
      :ok = Transport.send(server, resp)

      assert_receive {:ovsdb_message, ^client, %{"id" => 1, "result" => ["Open_vSwitch"]}},
                     1_000
    end

    test "bidirectional messages flow independently" do
      {client, server, on_exit} = make_pair()
      on_exit(on_exit)

      Transport.send(client, Protocol.request("list_dbs", [], 1))
      Transport.send(server, Protocol.notification("update", ["m", %{}]))

      assert_receive {:ovsdb_message, ^server, %{"method" => "list_dbs"}}, 1_000
      assert_receive {:ovsdb_message, ^client, %{"method" => "update"}}, 1_000
    end
  end

  describe "burst of messages" do
    test "10 back-to-back messages all framed and delivered in order" do
      {client, server, on_exit} = make_pair()
      on_exit(on_exit)

      for i <- 1..10 do
        Transport.send(client, Protocol.request("echo", ["ping#{i}"], i))
      end

      # Collect all 10 in order
      received =
        for i <- 1..10 do
          assert_receive {:ovsdb_message, ^server, %{"id" => ^i, "params" => params}}, 2_000
          params
        end

      expected = for i <- 1..10, do: ["ping#{i}"]
      assert received == expected
    end
  end

  describe "close behaviors" do
    test "close notifies the peer with :ovsdb_closed" do
      {client, server, on_exit} = make_pair()
      on_exit(on_exit)

      :ok = Transport.close(client)
      assert_receive {:ovsdb_closed, ^server}, 1_000
    end
  end

  describe "connect/3 — error cases" do
    test "returns error for unreachable port" do
      # Port 1 is usually either in use or requires root; connecting
      # should fail with :econnrefused or similar quickly.
      assert {:error, _reason} = Transport.connect("127.0.0.1", 1, timeout: 500)
    end

    test "returns error for nonexistent host" do
      assert {:error, _reason} =
               Transport.connect("nonexistent.invalid.local", 6640, timeout: 1_000)
    end
  end

  describe "set_controller/2" do
    test "changes the process that receives messages" do
      {client, server, on_exit} = make_pair()
      on_exit(on_exit)

      # Spawn a process that will await the message instead of us
      test_pid = self()

      worker =
        spawn_link(fn ->
          receive do
            msg -> Kernel.send(test_pid, {:worker_got, msg})
          end
        end)

      :ok = Transport.set_controller(server, worker)

      Transport.send(client, Protocol.request("list_dbs", [], 1))

      assert_receive {:worker_got, {:ovsdb_message, ^server, %{"method" => "list_dbs"}}}, 1_000
    end
  end
end
