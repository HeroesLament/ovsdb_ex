defmodule OVSDB.Server do
  @moduledoc """
  TCP/TLS acceptor for OVSDB servers. Accepts incoming connections
  and spawns a supervised `OVSDB.ServerSession` for each, dispatching
  protocol requests to a user-supplied `Handler` module.

  ## Architecture

      OVSDB.Server (GenServer)
      ├── listen socket
      ├── acceptor task (blocks on accept, hands off to supervisor)
      └── DynamicSupervisor
          ├── ServerSession #1 (Transport + Handler state)
          ├── ServerSession #2
          └── ...

  One `Server` process per listening port. Many `ServerSession`s per
  `Server` (one per client connection).

  ## Usage

      children = [
        {OVSDB.Server,
          port: 6640,
          handler: MySimHandler,
          handler_opts: [sim_pid: sim_pid]}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  ## Options

    * `:port` — TCP port to listen on (required)
    * `:transport` — `:gen_tcp` (default) or `:ssl`
    * `:ssl_opts` — TLS-specific options passed to `:ssl.listen/2`
    * `:handler` — module implementing `OVSDB.ServerSession.Handler` (required)
    * `:handler_opts` — opts passed to `handler.init/1` per connection
    * `:listen_opts` — extra opts merged into the listen call
      (e.g. `[ip: {127, 0, 0, 1}]` to bind to loopback only)
    * `:name` — name to register the Server process as

  ## Obtaining the bound port

  When `port: 0` is used (ephemeral), `listen_port/1` returns the
  actual port assigned by the OS. Useful for tests.
  """

  use GenServer

  require Logger

  alias OVSDB.ServerSession

  defstruct [
    :listen_socket,
    :transport_mod,
    :supervisor,
    :acceptor,
    :handler,
    :handler_opts
  ]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @type t :: pid() | atom()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc "Returns the actual port the server is bound to."
  @spec listen_port(t()) :: :inet.port_number() | {:error, term()}
  def listen_port(server), do: GenServer.call(server, :listen_port)

  @doc "Returns a list of active session pids."
  @spec sessions(t()) :: [pid()]
  def sessions(server), do: GenServer.call(server, :sessions)

  @doc "Stops the server, closing the listener and all sessions."
  @spec stop(t()) :: :ok
  def stop(server), do: GenServer.stop(server, :normal)

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    port = Keyword.fetch!(opts, :port)
    handler = Keyword.fetch!(opts, :handler)
    handler_opts = Keyword.get(opts, :handler_opts, [])
    transport_mod = Keyword.get(opts, :transport, :gen_tcp)
    listen_opts = Keyword.get(opts, :listen_opts, [])
    ssl_opts = Keyword.get(opts, :ssl_opts, [])

    with {:ok, listen_socket} <-
           do_listen(transport_mod, port, listen_opts, ssl_opts),
         {:ok, supervisor} <-
           DynamicSupervisor.start_link(strategy: :one_for_one) do
      state = %__MODULE__{
        listen_socket: listen_socket,
        transport_mod: transport_mod,
        supervisor: supervisor,
        handler: handler,
        handler_opts: handler_opts
      }

      # Start the acceptor loop; it runs independently and sends
      # us {:accepted, socket} messages.
      parent = self()

      acceptor =
        spawn_link(fn -> accept_loop(parent, transport_mod, listen_socket) end)

      {:ok, %{state | acceptor: acceptor}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:listen_port, _from, state) do
    port =
      case state.transport_mod do
        :gen_tcp ->
          case :inet.port(state.listen_socket) do
            {:ok, p} -> p
            err -> err
          end

        :ssl ->
          case :ssl.sockname(state.listen_socket) do
            {:ok, {_ip, p}} -> p
            err -> err
          end
      end

    {:reply, port, state}
  end

  def handle_call(:sessions, _from, state) do
    pids =
      DynamicSupervisor.which_children(state.supervisor)
      |> Enum.map(fn {_, pid, _, _} -> pid end)
      |> Enum.filter(&is_pid/1)

    {:reply, pids, state}
  end

  def handle_call({:spawn_session, socket}, _from, state) do
    child_spec = %{
      id: ServerSession,
      start:
        {ServerSession, :start_link,
         [
           socket,
           [
             transport: state.transport_mod,
             handler: state.handler,
             handler_opts: state.handler_opts
           ]
         ]},
      restart: :temporary
    }

    reply = DynamicSupervisor.start_child(state.supervisor, child_spec)
    {:reply, reply, state}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    _ = do_close_listen(state.transport_mod, state.listen_socket)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Private — acceptor loop
  #
  # The acceptor accepts sockets, starts a ServerSession child under the
  # DynamicSupervisor, then transfers socket ownership to the new session.
  # The acceptor is the socket owner until the transfer completes, so
  # this ordering is the only safe one: any attempt to do socket I/O
  # from the session before the transfer would fail with :not_owner.
  # ---------------------------------------------------------------------------

  defp accept_loop(parent, transport_mod, listen_socket) do
    case do_accept(transport_mod, listen_socket) do
      {:ok, socket} ->
        _ =
          case start_session(parent, transport_mod, socket) do
            {:ok, _session_pid} ->
              :ok

            {:error, reason} ->
              Logger.warning("OVSDB.Server failed to start session: #{inspect(reason)}")
              do_close_socket(transport_mod, socket)
          end

        accept_loop(parent, transport_mod, listen_socket)

      {:error, :closed} ->
        # Listen socket was closed; acceptor exits cleanly.
        :ok

      {:error, reason} ->
        Logger.warning("OVSDB.Server accept failed: #{inspect(reason)}")
        accept_loop(parent, transport_mod, listen_socket)
    end
  end

  # Ask the Server GenServer to spawn a ServerSession for `socket`,
  # then transfer ownership of the socket to the session.
  defp start_session(server, transport_mod, socket) do
    case GenServer.call(server, {:spawn_session, socket}) do
      {:ok, session_pid} ->
        # Transfer ownership. At this point the session exists but
        # its Transport.wrap hasn't been called yet — we're about to
        # cast it the go-ahead.
        case do_controlling_process(transport_mod, socket, session_pid) do
          :ok ->
            GenServer.cast(session_pid, :socket_owned)
            {:ok, session_pid}

          {:error, _} = err ->
            # Session exists but socket transfer failed; tell it to stop.
            _ = Process.exit(session_pid, :shutdown)
            err
        end

      {:error, _} = err ->
        err
    end
  end

  # ---------------------------------------------------------------------------
  # Private — transport shims
  # ---------------------------------------------------------------------------

  defp do_listen(:gen_tcp, port, listen_opts, _ssl_opts) do
    # These are proplists (gen_tcp option lists, not keyword lists)
    # because `:binary` is a bare atom not a key-value pair. Use ++
    # to concatenate; Keyword.merge would reject the `:binary` atom.
    opts =
      [
        :binary,
        {:active, false},
        {:packet, :raw},
        {:reuseaddr, true},
        {:backlog, 128},
        {:nodelay, true}
      ] ++ listen_opts

    :gen_tcp.listen(port, opts)
  end

  defp do_listen(:ssl, port, listen_opts, ssl_opts) do
    opts =
      [:binary, {:active, false}, {:reuseaddr, true}] ++ listen_opts ++ ssl_opts

    :ssl.listen(port, opts)
  end

  defp do_accept(:gen_tcp, lsock), do: :gen_tcp.accept(lsock)

  defp do_accept(:ssl, lsock) do
    with {:ok, sock} <- :ssl.transport_accept(lsock) do
      :ssl.handshake(sock)
    end
  end

  defp do_controlling_process(:gen_tcp, socket, pid),
    do: :gen_tcp.controlling_process(socket, pid)

  defp do_controlling_process(:ssl, socket, pid),
    do: :ssl.controlling_process(socket, pid)

  defp do_close_socket(:gen_tcp, socket), do: :gen_tcp.close(socket)
  defp do_close_socket(:ssl, socket), do: :ssl.close(socket)

  defp do_close_listen(:gen_tcp, socket), do: :gen_tcp.close(socket)
  defp do_close_listen(:ssl, socket), do: :ssl.close(socket)
end
