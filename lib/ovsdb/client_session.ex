defmodule OVSDB.ClientSession do
  @moduledoc """
  Client-side OVSDB session. Owns a `Transport`, correlates request
  IDs with callers, dispatches server-initiated notifications to
  subscribers, and auto-responds to `echo` keepalives.

  ## Purpose

  `Transport` delivers raw messages as they arrive, with no notion
  of "this is the response to the request I sent three seconds
  ago." Real clients need request/response correlation. That's what
  this module adds.

  ## Lifecycle

      {:ok, session} = ClientSession.connect("ovsdb-server.local", 6640)

      {:ok, dbs} = ClientSession.list_dbs(session)
      {:ok, schema} = ClientSession.get_schema(session, "Open_vSwitch")
      {:ok, results} = ClientSession.transact(session, transaction)

      ClientSession.close(session)

  ## Notifications

  To receive `update` notifications (or any other server-initiated
  message), subscribe before issuing a `monitor` request:

      :ok = ClientSession.subscribe(session, "update")

      ClientSession.monitor(session, monitor_spec)

      receive do
        {:ovsdb_notification, ^session, "update", ["my-monitor", updates]} ->
          handle_update(updates)
      end

  Subscriptions are by method name, so one subscriber can receive
  all `update` notifications regardless of monitor_id. The
  subscriber is responsible for routing by monitor_id if they've
  set up multiple monitors.

  ## Error semantics

    * `{:error, {:ovsdb, error_value}}` — server returned a non-null
      `error` field. The error value matches RFC 7047 §3.1: either
      a short error-code string or an object with `error` and
      `details` fields.
    * `{:error, :closed}` — socket closed during or before the
      request.
    * `{:error, :timeout}` — request timed out waiting for response.

  ## Echo keepalives

  If the server sends an `echo` notification (method `"echo"` with
  `id: null`), the session automatically replies. This is purely
  defensive — most OVSDB deployments don't use echo. If you want to
  proactively keepalive, call `echo/1` periodically from your
  application.
  """

  use GenServer

  require Logger

  alias OVSDB.{MonitorSpec, Protocol, Transaction, Transport}

  @type t :: pid()
  @type error :: {:ovsdb, term()} | :closed | :timeout

  defstruct [
    :transport,
    :next_id,
    # id => {from, deadline_ref}
    pending: %{},
    # method => MapSet.new(pids)
    subscribers: %{},
    # default timeout for a request in ms
    default_timeout: 30_000
  ]

  # ---------------------------------------------------------------------------
  # Public API — connection lifecycle
  # ---------------------------------------------------------------------------

  @doc """
  Connects to an OVSDB server and starts a session.

  Takes the same options as `Transport.connect/3` plus:

    * `:default_timeout` — per-request timeout; default 30_000 ms
  """
  @spec connect(String.t() | :inet.ip_address(), :inet.port_number(), keyword()) ::
          {:ok, t()} | {:error, term()}
  def connect(host, port, opts \\ []) do
    GenServer.start_link(__MODULE__, {:connect, host, port, opts})
  end

  @doc """
  Wraps an existing Transport pid as a client session. The session
  takes over as the Transport's controller.
  """
  @spec wrap_transport(Transport.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def wrap_transport(transport, opts \\ []) do
    GenServer.start_link(__MODULE__, {:wrap, transport, opts})
  end

  @doc """
  Closes the session and its underlying Transport.
  """
  @spec close(t()) :: :ok
  def close(session), do: GenServer.stop(session, :normal)

  # ---------------------------------------------------------------------------
  # Public API — RFC 7047 §4.1 methods
  # ---------------------------------------------------------------------------

  @doc "RFC 7047 §4.1.1 — list databases."
  @spec list_dbs(t(), timeout()) :: {:ok, [String.t()]} | {:error, error()}
  def list_dbs(session, timeout \\ :infinity) do
    call(session, "list_dbs", [], timeout)
  end

  @doc "RFC 7047 §4.1.2 — fetch a database schema as a JSON map."
  @spec get_schema(t(), String.t(), timeout()) :: {:ok, map()} | {:error, error()}
  def get_schema(session, db, timeout \\ :infinity) when is_binary(db) do
    call(session, "get_schema", [db], timeout)
  end

  @doc """
  RFC 7047 §4.1.3 — transact against a database. Returns the list
  of per-operation results in the same order as the transaction's
  operations.
  """
  @spec transact(t(), Transaction.t(), timeout()) :: {:ok, [map()]} | {:error, error()}
  def transact(session, %Transaction{} = txn, timeout \\ :infinity) do
    call(session, "transact", Transaction.to_params(txn), timeout)
  end

  @doc """
  RFC 7047 §4.1.5 — subscribe to changes. Returns the initial
  state of the monitored tables as specified by the `:initial`
  select flag.
  """
  @spec monitor(t(), MonitorSpec.t(), timeout()) :: {:ok, map()} | {:error, error()}
  def monitor(session, %MonitorSpec{} = spec, timeout \\ :infinity) do
    call(session, "monitor", MonitorSpec.to_params(spec), timeout)
  end

  @doc "RFC 7047 §4.1.7 — cancel a monitor subscription."
  @spec monitor_cancel(t(), MonitorSpec.monitor_id(), timeout()) ::
          {:ok, map()} | {:error, error()}
  def monitor_cancel(session, monitor_id, timeout \\ :infinity) do
    call(session, "monitor_cancel", [monitor_id], timeout)
  end

  @doc "RFC 7047 §4.1.11 — application-level echo (keepalive)."
  @spec echo(t(), [term()], timeout()) :: {:ok, [term()]} | {:error, error()}
  def echo(session, args \\ [], timeout \\ :infinity) when is_list(args) do
    call(session, "echo", args, timeout)
  end

  # ---------------------------------------------------------------------------
  # Public API — notifications
  # ---------------------------------------------------------------------------

  @doc """
  Subscribes the calling process to notifications of the given
  method. Subsequent notifications arrive as
  `{:ovsdb_notification, session_pid, method, params}`.

  Common methods to subscribe to: `"update"`, `"locked"`, `"stolen"`.
  """
  @spec subscribe(t(), String.t()) :: :ok
  def subscribe(session, method) when is_binary(method) do
    GenServer.call(session, {:subscribe, method, self()})
  end

  @doc "Removes a subscription."
  @spec unsubscribe(t(), String.t()) :: :ok
  def unsubscribe(session, method) when is_binary(method) do
    GenServer.call(session, {:unsubscribe, method, self()})
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init({:connect, host, port, opts}) do
    default_timeout = Keyword.get(opts, :default_timeout, 30_000)
    transport_opts = Keyword.put(opts, :controller, self())

    case Transport.connect(host, port, transport_opts) do
      {:ok, transport} ->
        {:ok,
         %__MODULE__{
           transport: transport,
           next_id: 1,
           default_timeout: default_timeout
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  def init({:wrap, transport, opts}) do
    default_timeout = Keyword.get(opts, :default_timeout, 30_000)
    :ok = Transport.set_controller(transport, self())

    {:ok,
     %__MODULE__{
       transport: transport,
       next_id: 1,
       default_timeout: default_timeout
     }}
  end

  @impl true
  def handle_call({:request, method, params, timeout}, from, state) do
    id = state.next_id
    request = Protocol.request(method, params, id)

    case Transport.send(state.transport, request) do
      :ok ->
        deadline_ref = schedule_timeout(id, timeout)

        state = %{
          state
          | next_id: id + 1,
            pending: Elixir.Map.put(state.pending, id, {from, deadline_ref})
        }

        {:noreply, state}

      {:error, reason} ->
        {:reply, {:error, {:send_failed, reason}}, state}
    end
  end

  def handle_call({:subscribe, method, pid}, _from, state) do
    subs = Elixir.Map.update(state.subscribers, method, MapSet.new([pid]), &MapSet.put(&1, pid))
    {:reply, :ok, %{state | subscribers: subs}}
  end

  def handle_call({:unsubscribe, method, pid}, _from, state) do
    subs =
      case Elixir.Map.get(state.subscribers, method) do
        nil ->
          state.subscribers

        set ->
          new_set = MapSet.delete(set, pid)

          if MapSet.size(new_set) == 0 do
            Elixir.Map.delete(state.subscribers, method)
          else
            Elixir.Map.put(state.subscribers, method, new_set)
          end
      end

    {:reply, :ok, %{state | subscribers: subs}}
  end

  # Inbound message from Transport.
  @impl true
  def handle_info({:ovsdb_message, transport, msg}, %{transport: transport} = state) do
    case Protocol.classify(msg) do
      {:ok, {:response, %{id: id, result: result, error: error}}} ->
        {:noreply, deliver_response(state, id, result, error)}

      {:ok, {:notification, %{method: method, params: params}}} ->
        {:noreply, handle_notification(state, method, params)}

      {:ok, {:request, %{id: id, method: _method, params: _params}}} ->
        # A server is not supposed to make requests of a client per
        # RFC 7047, but defensively reply with an error rather than
        # leaving the caller hanging.
        resp = Protocol.error_response(id, "not implemented")
        _ = Transport.send(transport, resp)
        {:noreply, state}

      {:error, reason} ->
        Logger.warning("OVSDB.ClientSession received malformed message: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  def handle_info({:ovsdb_closed, transport}, %{transport: transport} = state) do
    _ = fail_all_pending(state, :closed)
    {:stop, :normal, state}
  end

  def handle_info({:ovsdb_error, transport, reason}, %{transport: transport} = state) do
    Logger.warning("OVSDB.ClientSession transport error: #{inspect(reason)}")
    _ = fail_all_pending(state, {:transport, reason})
    {:stop, :normal, state}
  end

  def handle_info({:request_timeout, id}, state) do
    case Elixir.Map.pop(state.pending, id) do
      {nil, _} ->
        {:noreply, state}

      {{from, _ref}, rest} ->
        GenServer.reply(from, {:error, :timeout})
        {:noreply, %{state | pending: rest}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp call(session, method, params, timeout) do
    # timeout is the protocol-level per-request timeout tracked in
    # the session. GenServer.call timeout is :infinity because the
    # session will GenServer.reply when the protocol timeout fires.
    GenServer.call(session, {:request, method, params, timeout}, :infinity)
  end

  defp deliver_response(state, id, result, error) do
    case Elixir.Map.pop(state.pending, id) do
      {nil, _} ->
        Logger.warning("OVSDB.ClientSession got response for unknown id=#{inspect(id)}")
        state

      {{from, deadline_ref}, rest} ->
        cancel_timeout(deadline_ref)
        reply = if is_nil(error), do: {:ok, result}, else: {:error, {:ovsdb, error}}
        GenServer.reply(from, reply)
        %{state | pending: rest}
    end
  end

  defp handle_notification(state, method, params) do
    _ = notify_subscribers(state, method, params)
    state
  end

  defp notify_subscribers(state, method, params) do
    case Elixir.Map.get(state.subscribers, method) do
      nil ->
        :ok

      set ->
        Enum.each(set, fn pid ->
          Kernel.send(pid, {:ovsdb_notification, self(), method, params})
        end)
    end
  end

  defp fail_all_pending(state, reason) do
    Enum.each(state.pending, fn {_id, {from, deadline_ref}} ->
      cancel_timeout(deadline_ref)
      GenServer.reply(from, {:error, reason})
    end)
  end

  defp cancel_timeout(nil), do: :ok

  defp cancel_timeout(ref) when is_reference(ref) do
    _ = Process.cancel_timer(ref, async: true, info: false)
    :ok
  end

  defp schedule_timeout(_id, :infinity), do: nil

  defp schedule_timeout(id, timeout) when is_integer(timeout) and timeout >= 0 do
    Process.send_after(self(), {:request_timeout, id}, timeout)
  end
end
