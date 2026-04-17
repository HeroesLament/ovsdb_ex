defmodule OVSDB.ServerSession do
  @moduledoc """
  Per-connection server-side OVSDB session. Takes ownership of an
  accepted Transport, receives client requests, dispatches to a
  `Handler` behaviour module, and sends responses.

  ## Role asymmetry with ClientSession

  `ClientSession` originates requests and correlates responses.
  `ServerSession` does the inverse: receives requests, produces
  responses. It can also *originate* notifications (like `update`
  messages for monitors) via `notify/3`.

  ## Handler behaviour

  Applications implement `OVSDB.ServerSession.Handler` to respond to
  RFC 7047 methods. One handler module serves one server config;
  handler state is per-connection.

      defmodule MySimHandler do
        @behaviour OVSDB.ServerSession.Handler

        @impl true
        def init(opts), do: {:ok, %{sim_pid: Keyword.fetch!(opts, :sim_pid)}}

        @impl true
        def handle_list_dbs(state), do: {:ok, ["Open_vSwitch"], state}

        @impl true
        def handle_get_schema("Open_vSwitch", state) do
          {:ok, state.sim_pid |> Sim.schema(), state}
        end

        @impl true
        def handle_transact("Open_vSwitch", ops, state) do
          # Apply ops, return per-op results
        end

        # ...
      end

  ## Unimplemented methods

  Handler callbacks are all optional. A method with no matching
  callback returns `{"error": "not supported"}` per RFC 7047 §4 —
  the default behavior of `Handler`'s default implementations.

  ## Notifications

  `notify/3` pushes a notification to the connected client:

      ServerSession.notify(session, "update", ["monitor-id", updates])
  """

  use GenServer

  require Logger

  alias OVSDB.{Protocol, Transport}

  defmodule Handler do
    @moduledoc """
    Behaviour for applications serving OVSDB requests.

    All callbacks are optional. Methods not implemented return a
    `"not supported"` error to the client.
    """

    @type state :: term()
    @type error_string :: String.t()
    @type result :: {:ok, term(), state()} | {:error, error_string(), state()}

    @callback init(opts :: keyword()) :: {:ok, state()} | {:error, term()}
    @callback terminate(reason :: term(), state()) :: :ok

    @callback handle_list_dbs(state()) :: result()
    @callback handle_get_schema(db :: String.t(), state()) :: result()
    @callback handle_transact(db :: String.t(), ops :: [map()], state()) :: result()
    @callback handle_cancel(id :: term(), state()) :: result()
    @callback handle_monitor(
                db :: String.t(),
                monitor_id :: term(),
                requests :: map(),
                state()
              ) :: result()
    @callback handle_monitor_cancel(monitor_id :: term(), state()) :: result()
    @callback handle_lock(lock :: String.t(), state()) :: result()
    @callback handle_steal(lock :: String.t(), state()) :: result()
    @callback handle_unlock(lock :: String.t(), state()) :: result()
    @callback handle_echo(args :: [term()], state()) :: result()

    @optional_callbacks terminate: 2,
                        handle_list_dbs: 1,
                        handle_get_schema: 2,
                        handle_transact: 3,
                        handle_cancel: 2,
                        handle_monitor: 4,
                        handle_monitor_cancel: 2,
                        handle_lock: 2,
                        handle_steal: 2,
                        handle_unlock: 2,
                        handle_echo: 2
  end

  @type t :: pid()

  defstruct [
    :transport,
    :handler_mod,
    :handler_state,
    # Used during the window between start_link and :socket_owned cast.
    :pending_socket,
    :pending_transport_mod
  ]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts a ServerSession on an accepted socket. Wraps the socket
  in a Transport owned by the session.

  ## Options

    * `:transport` — `:gen_tcp` (default) or `:ssl`
    * `:handler` — module implementing `Handler` behaviour (required)
    * `:handler_opts` — options passed to `handler.init/1`
  """
  @spec start_link(port() | :ssl.sslsocket(), keyword()) ::
          {:ok, t()} | {:error, term()}
  def start_link(socket, opts) do
    GenServer.start_link(__MODULE__, {socket, opts})
  end

  @doc """
  Sends a notification to the connected client.

  Implemented as a cast so it's safe to call from within a handler
  callback (which runs in the session's own process — a synchronous
  `GenServer.call` would deadlock). Failures to send are logged;
  callers that need delivery guarantees should not use this path.
  """
  @spec notify(t(), String.t(), list()) :: :ok
  def notify(session, method, params) when is_binary(method) and is_list(params) do
    GenServer.cast(session, {:notify, method, params})
  end

  @doc "Stops the session (closes the connection)."
  @spec close(t()) :: :ok
  def close(session), do: GenServer.stop(session, :normal)

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init({socket, opts}) do
    handler_mod = Keyword.fetch!(opts, :handler)
    handler_opts = Keyword.get(opts, :handler_opts, [])
    transport_mod = Keyword.get(opts, :transport, :gen_tcp)

    case handler_mod.init(handler_opts) do
      {:ok, handler_state} ->
        # Don't touch the socket yet — the acceptor still owns it.
        # Wait for the :socket_owned cast before calling Transport.wrap.
        {:ok,
         %__MODULE__{
           transport: nil,
           handler_mod: handler_mod,
           handler_state: handler_state,
           pending_socket: socket,
           pending_transport_mod: transport_mod
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_cast(:socket_owned, %{pending_socket: socket, pending_transport_mod: tm} = state) do
    case Transport.wrap(socket, transport: tm, controller: self()) do
      {:ok, transport} ->
        {:noreply, %{state | transport: transport, pending_socket: nil, pending_transport_mod: nil}}

      {:error, reason} ->
        Logger.warning("OVSDB.ServerSession failed to wrap socket: #{inspect(reason)}")
        {:stop, :normal, state}
    end
  end

  def handle_cast({:notify, method, params}, state) do
    notification = Protocol.notification(method, params)

    case Transport.send(state.transport, notification) do
      :ok ->
        {:noreply, state}

      {:error, reason} ->
        Logger.warning("OVSDB.ServerSession notify send failed: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:ovsdb_message, transport, msg}, %{transport: transport} = state) do
    case Protocol.classify(msg) do
      {:ok, {:request, %{id: id, method: method, params: params}}} ->
        {:noreply, dispatch_request(state, id, method, params)}

      {:ok, {:notification, %{method: "echo", params: params}}} ->
        # Client keepalive — reply with echo notification back.
        _ = Transport.send(transport, Protocol.notification("echo", params))
        {:noreply, state}

      {:ok, {:notification, %{method: method, params: _params}}} ->
        Logger.debug("OVSDB.ServerSession ignoring notification: #{method}")
        {:noreply, state}

      {:ok, {:response, _}} ->
        # Servers don't typically send requests that expect responses.
        Logger.warning("OVSDB.ServerSession got unexpected response")
        {:noreply, state}

      {:error, reason} ->
        Logger.warning("OVSDB.ServerSession received malformed: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  def handle_info({:ovsdb_closed, transport}, %{transport: transport} = state) do
    {:stop, :normal, state}
  end

  def handle_info({:ovsdb_error, transport, reason}, %{transport: transport} = state) do
    Logger.warning("OVSDB.ServerSession transport error: #{inspect(reason)}")
    {:stop, :normal, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(reason, state) do
    if function_exported?(state.handler_mod, :terminate, 2) do
      _ = state.handler_mod.terminate(reason, state.handler_state)
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # Private — request dispatch
  # ---------------------------------------------------------------------------

  defp dispatch_request(state, id, method, params) do
    case do_dispatch(state.handler_mod, state.handler_state, method, params) do
      {:ok, result, new_state} ->
        send_response(state, Protocol.response(id, result))
        %{state | handler_state: new_state}

      {:error, error_string, new_state} ->
        send_response(state, Protocol.error_response(id, error_string))
        %{state | handler_state: new_state}

      :not_supported ->
        send_response(state, Protocol.error_response(id, "not supported"))
        state
    end
  end

  defp send_response(state, response) do
    case Transport.send(state.transport, response) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("OVSDB.ServerSession failed to send response: #{inspect(reason)}")
    end
  end

  # Route methods to handler callbacks. Each method's expected
  # param shape is per RFC 7047 §4.1.
  defp do_dispatch(mod, state, "list_dbs", []) do
    if function_exported?(mod, :handle_list_dbs, 1) do
      mod.handle_list_dbs(state)
    else
      :not_supported
    end
  end

  defp do_dispatch(mod, state, "get_schema", [db]) when is_binary(db) do
    if function_exported?(mod, :handle_get_schema, 2) do
      mod.handle_get_schema(db, state)
    else
      :not_supported
    end
  end

  defp do_dispatch(mod, state, "transact", [db | ops]) when is_binary(db) do
    if function_exported?(mod, :handle_transact, 3) do
      mod.handle_transact(db, ops, state)
    else
      :not_supported
    end
  end

  defp do_dispatch(mod, state, "cancel", [id]) do
    if function_exported?(mod, :handle_cancel, 2) do
      mod.handle_cancel(id, state)
    else
      :not_supported
    end
  end

  defp do_dispatch(mod, state, "monitor", [db, monitor_id, requests])
       when is_binary(db) and is_map(requests) do
    if function_exported?(mod, :handle_monitor, 4) do
      mod.handle_monitor(db, monitor_id, requests, state)
    else
      :not_supported
    end
  end

  defp do_dispatch(mod, state, "monitor_cancel", [monitor_id]) do
    if function_exported?(mod, :handle_monitor_cancel, 2) do
      mod.handle_monitor_cancel(monitor_id, state)
    else
      :not_supported
    end
  end

  defp do_dispatch(mod, state, "lock", [lock]) when is_binary(lock) do
    if function_exported?(mod, :handle_lock, 2),
      do: mod.handle_lock(lock, state),
      else: :not_supported
  end

  defp do_dispatch(mod, state, "steal", [lock]) when is_binary(lock) do
    if function_exported?(mod, :handle_steal, 2),
      do: mod.handle_steal(lock, state),
      else: :not_supported
  end

  defp do_dispatch(mod, state, "unlock", [lock]) when is_binary(lock) do
    if function_exported?(mod, :handle_unlock, 2),
      do: mod.handle_unlock(lock, state),
      else: :not_supported
  end

  defp do_dispatch(mod, state, "echo", args) when is_list(args) do
    if function_exported?(mod, :handle_echo, 2) do
      mod.handle_echo(args, state)
    else
      # Echo has a sensible default: return the args unchanged.
      {:ok, args, state}
    end
  end

  defp do_dispatch(_mod, _state, _method, _params), do: :not_supported
end
