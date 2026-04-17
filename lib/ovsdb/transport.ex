defmodule OVSDB.Transport do
  @moduledoc """
  Socket owner for an OVSDB wire connection. Handles framing,
  flow control, and delivery of decoded messages to a controller
  process.

  ## Role-neutral

  The same process works for both client-side connections (outbound,
  started via `connect/3`) and server-side connections (inbound,
  started via `wrap/2` from an already-accepted socket). Direction
  only matters at creation time; afterward the conversation is
  symmetric — either side may send requests and receive responses
  and notifications.

  ## Delivery model

  Inbound messages are delivered to the controlling process as:

      {:ovsdb_message, transport_pid, decoded_map}

  Framing or decode errors:

      {:ovsdb_error, transport_pid, reason}

  Remote close:

      {:ovsdb_closed, transport_pid}

  The controller is the process that called `connect/3` or `wrap/2`,
  or whichever process most recently called `set_controller/2`. This
  mirrors the `:gen_tcp` `controlling_process/2` pattern — one
  designated recipient for all traffic.

  ## Flow control

  The socket runs in `active: :once` mode: at most one incoming
  TCP packet sits in the transport's mailbox at a time. After
  processing it, the transport re-arms. This backpressures the
  sender if the controller is slow, rather than letting the
  transport's mailbox grow unboundedly.

  ## TLS

  Pass `transport: :ssl` in opts to use TLS instead of plain TCP.
  TLS-specific options (certfile, keyfile, cacertfile, verify, etc.)
  are passed through to `:ssl.connect/3`.

      OVSDB.Transport.connect("mgr.osync.local", 6640,
        transport: :ssl,
        ssl_opts: [
          certfile: "/etc/ssl/node.crt",
          keyfile: "/etc/ssl/node.key",
          cacertfile: "/etc/ssl/ca.crt",
          verify: :verify_peer
        ])
  """

  use GenServer

  require Logger

  alias OVSDB.{Framer, Protocol}

  @type t :: pid()
  @type transport_mod :: :gen_tcp | :ssl

  defstruct [
    :socket,
    :transport_mod,
    :controller,
    framer: nil
  ]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Opens an outbound connection to an OVSDB server.

  ## Options

    * `:transport` — `:gen_tcp` (default) or `:ssl`
    * `:ssl_opts` — passed through to `:ssl.connect/3` when TLS is used
    * `:connect_timeout` — milliseconds; default 5000
    * `:controller` — pid to receive messages; default `self()`

  ## Returns

  `{:ok, transport_pid}` on success. The calling process (or the one
  specified in `:controller`) will start receiving
  `{:ovsdb_message, transport_pid, map}` messages.

  `{:error, reason}` if the connection fails.
  """
  @spec connect(String.t() | :inet.ip_address(), :inet.port_number(), keyword()) ::
          {:ok, t()} | {:error, term()}
  def connect(host, port, opts \\ []) do
    opts = Keyword.put_new(opts, :controller, self())
    GenServer.start_link(__MODULE__, {:connect, host, port, opts})
  end

  @doc """
  Wraps an already-accepted socket. Used by `OVSDB.Server` after
  accepting a connection, or by any caller who holds a socket and
  wants a Transport to own it.

  **The caller must be the current controlling process of the
  socket.** `wrap/2` performs the ownership handoff internally:
  spawns the Transport, transfers socket ownership from the caller
  to the Transport, then signals the Transport to begin reading.

  ## Options

    * `:transport` — `:gen_tcp` (default) or `:ssl`
    * `:controller` — pid to receive messages; default `self()`
  """
  @spec wrap(port() | :ssl.sslsocket(), keyword()) :: {:ok, t()} | {:error, term()}
  def wrap(socket, opts \\ []) do
    opts = Keyword.put_new(opts, :controller, self())

    with {:ok, transport} <- GenServer.start_link(__MODULE__, {:wrap, socket, opts}),
         transport_mod = Keyword.get(opts, :transport, :gen_tcp),
         :ok <- do_controlling_process(transport_mod, socket, transport) do
      GenServer.cast(transport, :socket_owned)
      {:ok, transport}
    end
  end

  @doc """
  Sends a message map on the socket. Returns `:ok` or
  `{:error, reason}`. The message is serialized via
  `OVSDB.Protocol.serialize/1`.
  """
  @spec send(t(), Protocol.message()) :: :ok | {:error, term()}
  def send(transport, message) when is_map(message) do
    GenServer.call(transport, {:send, message})
  end

  @doc """
  Changes the controlling process. Subsequent inbound messages
  will be delivered to `new_controller`.
  """
  @spec set_controller(t(), pid()) :: :ok
  def set_controller(transport, new_controller) when is_pid(new_controller) do
    GenServer.call(transport, {:set_controller, new_controller})
  end

  @doc """
  Closes the socket and stops the transport.
  """
  @spec close(t()) :: :ok
  def close(transport) do
    GenServer.stop(transport, :normal)
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init({:connect, host, port, opts}) do
    transport_mod = Keyword.get(opts, :transport, :gen_tcp)
    controller = Keyword.fetch!(opts, :controller)
    timeout = Keyword.get(opts, :connect_timeout, 5_000)

    with {:ok, socket} <- do_connect(transport_mod, host, port, opts, timeout),
         :ok <- do_setopts(transport_mod, socket, active: :once) do
      state = %__MODULE__{
        socket: socket,
        transport_mod: transport_mod,
        controller: controller,
        framer: Framer.new()
      }

      {:ok, state}
    end
  end

  @impl true
  def init({:wrap, socket, opts}) do
    transport_mod = Keyword.get(opts, :transport, :gen_tcp)
    controller = Keyword.fetch!(opts, :controller)

    # Do NOT call setopts here — the caller is still the socket's
    # controlling process. We defer socket I/O until `wrap/2` has
    # transferred ownership and sent us the :socket_owned cast.
    state = %__MODULE__{
      socket: socket,
      transport_mod: transport_mod,
      controller: controller,
      framer: Framer.new()
    }

    {:ok, state}
  end

  @impl true
  def handle_cast(:socket_owned, state) do
    case do_setopts(state.transport_mod, state.socket, active: :once) do
      :ok ->
        {:noreply, state}

      {:error, reason} ->
        Logger.warning(
          "OVSDB.Transport setopts after ownership transfer failed: #{inspect(reason)}"
        )

        {:stop, :normal, state}
    end
  end

  @impl true
  def handle_call({:send, message}, _from, state) do
    iodata = Protocol.serialize(message)

    case do_send(state.transport_mod, state.socket, iodata) do
      :ok -> {:reply, :ok, state}
      {:error, _reason} = err -> {:reply, err, state}
    end
  end

  @impl true
  def handle_call({:set_controller, new_controller}, _from, state) do
    {:reply, :ok, %{state | controller: new_controller}}
  end

  # TCP data
  @impl true
  def handle_info({:tcp, socket, data}, %{socket: socket, transport_mod: :gen_tcp} = state) do
    handle_incoming(state, data)
  end

  def handle_info({:ssl, socket, data}, %{socket: socket, transport_mod: :ssl} = state) do
    handle_incoming(state, data)
  end

  # Socket closed
  def handle_info({:tcp_closed, socket}, %{socket: socket} = state) do
    notify(state, {:ovsdb_closed, self()})
    {:stop, :normal, state}
  end

  def handle_info({:ssl_closed, socket}, %{socket: socket} = state) do
    notify(state, {:ovsdb_closed, self()})
    {:stop, :normal, state}
  end

  # Socket error
  def handle_info({:tcp_error, socket, reason}, %{socket: socket} = state) do
    notify(state, {:ovsdb_error, self(), {:socket, reason}})
    {:stop, :normal, state}
  end

  def handle_info({:ssl_error, socket, reason}, %{socket: socket} = state) do
    notify(state, {:ovsdb_error, self(), {:socket, reason}})
    {:stop, :normal, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    _ = do_close(state.transport_mod, state.socket)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp handle_incoming(state, data) do
    case Framer.feed(state.framer, data) do
      {:error, reason, framer} ->
        notify(state, {:ovsdb_error, self(), {:framer, reason}})
        {:stop, :normal, %{state | framer: framer}}

      {framer, messages} ->
        :ok = dispatch_messages(state, messages)
        :ok = do_setopts(state.transport_mod, state.socket, active: :once)
        {:noreply, %{state | framer: framer}}
    end
  end

  defp dispatch_messages(state, messages) do
    Enum.each(messages, fn raw ->
      case Jason.decode(raw) do
        {:ok, map} when is_map(map) ->
          notify(state, {:ovsdb_message, self(), map})

        {:ok, _non_map} ->
          notify(state, {:ovsdb_error, self(), :not_an_object})

        {:error, reason} ->
          notify(state, {:ovsdb_error, self(), {:decode, reason}})
      end
    end)
  end

  defp notify(%{controller: pid}, message) when is_pid(pid) do
    Kernel.send(pid, message)
  end

  # Socket ops — thin shims over :gen_tcp / :ssl.

  defp do_connect(:gen_tcp, host, port, _opts, timeout) do
    :gen_tcp.connect(
      to_charlist_host(host),
      port,
      [:binary, {:active, false}, {:packet, :raw}, {:nodelay, true}],
      timeout
    )
  end

  defp do_connect(:ssl, host, port, opts, timeout) do
    ssl_opts = Keyword.get(opts, :ssl_opts, [])
    merged = [:binary, {:active, false}] ++ ssl_opts
    :ssl.connect(to_charlist_host(host), port, merged, timeout)
  end

  defp do_setopts(:gen_tcp, socket, opts), do: :inet.setopts(socket, opts)
  defp do_setopts(:ssl, socket, opts), do: :ssl.setopts(socket, opts)

  defp do_send(:gen_tcp, socket, data), do: :gen_tcp.send(socket, data)
  defp do_send(:ssl, socket, data), do: :ssl.send(socket, data)

  defp do_close(:gen_tcp, socket), do: :gen_tcp.close(socket)
  defp do_close(:ssl, socket), do: :ssl.close(socket)

  defp do_controlling_process(:gen_tcp, socket, pid),
    do: :gen_tcp.controlling_process(socket, pid)

  defp do_controlling_process(:ssl, socket, pid),
    do: :ssl.controlling_process(socket, pid)

  defp to_charlist_host(h) when is_binary(h), do: String.to_charlist(h)
  defp to_charlist_host(h) when is_list(h), do: h
  defp to_charlist_host(h) when is_tuple(h), do: h
end
