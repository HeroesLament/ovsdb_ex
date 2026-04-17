defmodule OVSDB.Protocol do
  @moduledoc """
  JSON-RPC 1.0 envelope handling for OVSDB (RFC 7047 §4).

  This module has one job: build, classify, serialize, and parse the
  three kinds of JSON-RPC 1.0 messages that OVSDB uses. It does NOT
  know about:

    * The meaning of any specific method (`list_dbs`, `transact`, etc.)
      — that's `OVSDB.Operation` and the method-specific
      builders in Layer 3.
    * OVSDB value encoding (UUID wire form, set tagging, etc.) — the
      caller is expected to pass JSON-ready terms in `params` and
      `result`. Use `OVSDB.Value.encode/1` to prepare them.
    * Socket IO or message framing — that's `OVSDB.Transport`.
    * Request/response correlation or session state — that's
      `OVSDB.Session`.

  ## Message shapes (RFC 7047 §4 + JSON-RPC 1.0)

  **Request** — client invokes a method on the server:

      %{"method" => "list_dbs", "params" => [], "id" => 42}

  **Response** — server replies to a request. Exactly one of `result`
  and `error` is non-null:

      %{"result" => [...], "error" => nil, "id" => 42}
      %{"result" => nil, "error" => "unknown database", "id" => 42}

  **Notification** — either side sends a one-way message. `id` is
  explicitly null, which is how notifications are distinguished from
  requests:

      %{"method" => "update", "params" => [...], "id" => nil}

  ## Design philosophy

  All the building and classification functions work on plain Elixir
  maps. There is no OVSDB-specific struct involved; the wire form
  maps directly to Elixir maps with string keys, and that's what
  flows through the whole library. This makes it easy to inspect,
  test, and match on messages without import ceremony.

  Jason is touched only in `serialize/1` and `parse/1`. Everything
  else is pure Elixir term manipulation.
  """

  @type id :: non_neg_integer() | String.t()

  @type request :: %{
          required(String.t()) => term(),
          optional(String.t()) => term()
        }

  @type response :: %{
          required(String.t()) => term()
        }

  @type notification :: %{
          required(String.t()) => term()
        }

  @type message :: request() | response() | notification()

  @type classified ::
          {:request, %{id: id(), method: String.t(), params: list()}}
          | {:response, %{id: id(), result: term() | nil, error: term() | nil}}
          | {:notification, %{method: String.t(), params: list()}}

  @typedoc """
  Errors that classification and parsing may return. The set is
  closed — new error atoms will be added via a minor or major
  version bump.
  """
  @type classify_error ::
          :unclassifiable
          | :response_id_null
          | :response_result_and_error_both_null
          | :response_result_and_error_both_set
          | {:not_a_map, term()}
          | {:missing_fields, [String.t()]}
          | {:malformed_method_message, map()}

  @typedoc """
  Errors that `parse/1` may return, beyond those `classify_error`
  covers when composed via `parse_and_classify/1`.
  """
  @type parse_error ::
          :not_an_object
          | {:parse_error, Jason.DecodeError.t()}

  # RFC 7047 §4.1 — the only methods OVSDB defines.
  @client_methods ~w(list_dbs get_schema transact cancel monitor monitor_cancel
                     lock steal unlock echo)

  @server_notifications ~w(update locked stolen echo)

  @methods @client_methods ++ @server_notifications

  @doc """
  Returns the list of all method names defined by RFC 7047.
  """
  @spec known_methods() :: [String.t()]
  def known_methods, do: Enum.uniq(@methods)

  @doc """
  Returns `true` if `method` is a method name defined by RFC 7047.

      iex> OVSDB.Protocol.known_method?("transact")
      true

      iex> OVSDB.Protocol.known_method?("something_else")
      false
  """
  @spec known_method?(String.t()) :: boolean()
  def known_method?(method) when is_binary(method), do: method in @methods
  def known_method?(_), do: false

  # ---------------------------------------------------------------------------
  # Builders
  # ---------------------------------------------------------------------------

  @doc """
  Builds a request message.

  `id` must be non-null (a request with null id is a notification).
  `params` must be a list (may be empty). `method` should be one of
  the RFC 7047 methods but is not enforced — this function accepts
  any string, since OVSDB implementations sometimes add vendor
  extensions.

      iex> OVSDB.Protocol.request("list_dbs", [], 1)
      %{"method" => "list_dbs", "params" => [], "id" => 1}

      iex> OVSDB.Protocol.request("get_schema", ["Open_vSwitch"], "my-id")
      %{"method" => "get_schema", "params" => ["Open_vSwitch"], "id" => "my-id"}
  """
  @spec request(String.t(), list(), id()) :: request()
  def request(method, params, id)
      when is_binary(method) and is_list(params) and not is_nil(id) do
    %{"method" => method, "params" => params, "id" => id}
  end

  @doc """
  Builds a successful response message.

      iex> OVSDB.Protocol.response(1, ["Open_vSwitch"])
      %{"result" => ["Open_vSwitch"], "error" => nil, "id" => 1}
  """
  @spec response(id(), term()) :: response()
  def response(id, result) when not is_nil(id) do
    %{"result" => result, "error" => nil, "id" => id}
  end

  @doc """
  Builds an error response message.

  The `error` string should be a short OVSDB-defined error code
  (e.g. `"unknown database"`, `"canceled"`). An optional `details`
  string may carry a longer human-readable explanation, per
  RFC 7047 §3.1's `<e>` grammar.

      iex> OVSDB.Protocol.error_response(1, "unknown database")
      %{"result" => nil, "error" => "unknown database", "id" => 1}

      iex> OVSDB.Protocol.error_response(1, "syntax error", "unexpected token")
      %{
        "result" => nil,
        "error" => %{"error" => "syntax error", "details" => "unexpected token"},
        "id" => 1
      }
  """
  @spec error_response(id(), String.t(), String.t() | nil) :: response()
  def error_response(id, error, details \\ nil) when not is_nil(id) and is_binary(error) do
    error_value =
      case details do
        nil -> error
        d when is_binary(d) -> %{"error" => error, "details" => d}
      end

    %{"result" => nil, "error" => error_value, "id" => id}
  end

  @doc """
  Builds a notification message (one-way, no response expected).

      iex> OVSDB.Protocol.notification("echo", ["keepalive"])
      %{"method" => "echo", "params" => ["keepalive"], "id" => nil}

      iex> OVSDB.Protocol.notification("update", ["my_monitor", %{}])
      %{"method" => "update", "params" => ["my_monitor", %{}], "id" => nil}
  """
  @spec notification(String.t(), list()) :: notification()
  def notification(method, params) when is_binary(method) and is_list(params) do
    %{"method" => method, "params" => params, "id" => nil}
  end

  # ---------------------------------------------------------------------------
  # Classification
  # ---------------------------------------------------------------------------

  @doc """
  Classifies a parsed message map into its canonical form.

  Returns `{:request, ...}`, `{:response, ...}`, or
  `{:notification, ...}` with the keys extracted into atom-keyed
  struct-ish maps for convenient pattern matching. Returns
  `{:error, reason}` for malformed messages.

  ## Classification rules (RFC 7047 §4 + JSON-RPC 1.0)

    * If `method` is present AND `id` is non-null → **request**
    * If `method` is present AND `id` is null → **notification**
    * If `method` is absent (i.e. `result`/`error` present) → **response**

  The server MUST send exactly one of `result` or `error` as null in
  a response; this function validates that invariant.

      iex> OVSDB.Protocol.classify(%{"method" => "list_dbs", "params" => [], "id" => 1})
      {:ok, {:request, %{id: 1, method: "list_dbs", params: []}}}

      iex> OVSDB.Protocol.classify(%{"method" => "update", "params" => [], "id" => nil})
      {:ok, {:notification, %{method: "update", params: []}}}

      iex> OVSDB.Protocol.classify(%{"result" => [], "error" => nil, "id" => 1})
      {:ok, {:response, %{id: 1, result: [], error: nil}}}

      iex> OVSDB.Protocol.classify(%{"method" => "x"})
      {:error, {:missing_fields, ["params", "id"]}}
  """
  @spec classify(term()) :: {:ok, classified()} | {:error, classify_error()}
  def classify(message) when is_map(message) do
    case {
      Elixir.Map.has_key?(message, "method"),
      Elixir.Map.has_key?(message, "params"),
      Elixir.Map.has_key?(message, "id"),
      Elixir.Map.has_key?(message, "result"),
      Elixir.Map.has_key?(message, "error")
    } do
      {true, true, true, _, _} ->
        classify_method_bearing(message)

      {true, _, _, _, _} ->
        missing =
          [{"params", Elixir.Map.has_key?(message, "params")}, {"id", Elixir.Map.has_key?(message, "id")}]
          |> Enum.reject(fn {_, present} -> present end)
          |> Enum.map(fn {name, _} -> name end)

        {:error, {:missing_fields, missing}}

      {false, _, true, true, true} ->
        classify_response(message)

      {false, _, _, _, _} ->
        {:error, :unclassifiable}
    end
  end

  def classify(other), do: {:error, {:not_a_map, other}}

  defp classify_method_bearing(%{"method" => method, "params" => params, "id" => id})
       when is_binary(method) and is_list(params) do
    case id do
      nil -> {:ok, {:notification, %{method: method, params: params}}}
      _ -> {:ok, {:request, %{id: id, method: method, params: params}}}
    end
  end

  defp classify_method_bearing(msg) do
    {:error, {:malformed_method_message, msg}}
  end

  defp classify_response(%{"id" => id, "result" => result, "error" => error}) do
    cond do
      is_nil(id) ->
        {:error, :response_id_null}

      is_nil(result) and is_nil(error) ->
        {:error, :response_result_and_error_both_null}

      not is_nil(result) and not is_nil(error) ->
        {:error, :response_result_and_error_both_set}

      true ->
        {:ok, {:response, %{id: id, result: result, error: error}}}
    end
  end

  # ---------------------------------------------------------------------------
  # Wire I/O
  # ---------------------------------------------------------------------------

  @doc """
  Serializes a message map to iodata suitable for writing to the wire.

  Does not add any framing — OVSDB sends bare JSON objects
  back-to-back on the stream with no length prefix or delimiter.
  Message boundaries are found by the receiver via JSON
  brace-matching.

      iex> req = OVSDB.Protocol.request("list_dbs", [], 1)
      iex> iodata = OVSDB.Protocol.serialize(req)
      iex> IO.iodata_to_binary(iodata)
      "{\\"id\\":1,\\"method\\":\\"list_dbs\\",\\"params\\":[]}"
  """
  @spec serialize(message()) :: iodata()
  def serialize(message) when is_map(message) do
    Jason.encode_to_iodata!(message)
  end

  @doc """
  Parses one complete JSON object from a binary into a message map.

  The caller is responsible for message framing — this function
  expects exactly one JSON object and does not tolerate trailing
  bytes. Returns `{:ok, map}` on success, `{:error, reason}` on
  parse failure or if the parsed value is not a JSON object.

      iex> {:ok, msg} = OVSDB.Protocol.parse(~s({"method":"list_dbs","params":[],"id":1}))
      iex> msg
      %{"method" => "list_dbs", "params" => [], "id" => 1}

      iex> {:error, {:parse_error, _reason}} = OVSDB.Protocol.parse("not json")
      iex> :ok
      :ok

      iex> OVSDB.Protocol.parse("[1,2,3]")
      {:error, :not_an_object}
  """
  @spec parse(binary()) :: {:ok, message()} | {:error, parse_error()}
  def parse(binary) when is_binary(binary) do
    case Jason.decode(binary) do
      {:ok, map} when is_map(map) -> {:ok, map}
      {:ok, _non_map} -> {:error, :not_an_object}
      {:error, reason} -> {:error, {:parse_error, reason}}
    end
  end

  @doc """
  Convenience: parse a binary then classify it in one step.

  Returns the same `{:ok, classified()}` / `{:error, term()}` shape
  as `classify/1`, with parse errors subsumed.
  """
  @spec parse_and_classify(binary()) ::
          {:ok, classified()} | {:error, parse_error() | classify_error()}
  def parse_and_classify(binary) when is_binary(binary) do
    with {:ok, map} <- parse(binary) do
      classify(map)
    end
  end
end
