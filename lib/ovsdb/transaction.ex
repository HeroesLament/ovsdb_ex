defmodule OVSDB.Transaction do
  @moduledoc """
  Accumulator for the operations that make up a `transact` request.

  Per [RFC 7047 §4.1.3][rfc-transact], a `transact` request's params
  are `[<db-name>, <op1>, <op2>, ...]` — the database name followed
  by an ordered list of operations.

  [rfc-transact]: https://www.rfc-editor.org/rfc/rfc7047#section-4.1.3

  ## Usage

      alias OVSDB.{Transaction, Operation, Condition, NamedUUID}

      txn =
        Transaction.new("Open_vSwitch")
        |> Transaction.add(Operation.insert("Bridge", %{"name" => "br-lan"}, uuid_name: "new_br"))
        |> Transaction.add(Operation.insert("Port",
             %{"name" => "eth0", "bridge" => NamedUUID.new("new_br")}))
        |> Transaction.add(Operation.comment("wire up br-lan/eth0"))

      params = Transaction.to_params(txn)
      # → ["Open_vSwitch", %{...insert bridge...}, %{...insert port...}, %{...comment...}]

      request = OVSDB.Protocol.request("transact", params, request_id)

  ## Order matters

  Per RFC 7047 §5.2, operations are executed **in order**, and a
  failure in any operation aborts the entire transaction. `add/2`
  appends to the end — the order you call it determines execution
  order. Use `prepend/2` if you specifically need to insert at the
  front (rare).

  ## No rollback state

  A `Transaction` struct is a pure value. There is no session
  attached, no in-flight correlation, no commit/rollback state. It
  is merely a builder for the params list. Execution happens when
  the caller hands the params to `OVSDB.Session.transact/2` (or
  whatever transport is in use).
  """

  alias OVSDB.Operation

  @enforce_keys [:db]
  defstruct db: nil, ops: []

  @type t :: %__MODULE__{
          db: String.t(),
          ops: [Operation.t()]
        }

  @doc """
  Creates a new empty transaction targeting the given database.

      iex> OVSDB.Transaction.new("Open_vSwitch")
      %OVSDB.Transaction{db: "Open_vSwitch", ops: []}
  """
  @spec new(String.t()) :: t()
  def new(db) when is_binary(db), do: %__MODULE__{db: db, ops: []}

  @doc """
  Appends an operation to the transaction. Returns the updated
  transaction so calls can be chained.

      iex> txn = OVSDB.Transaction.new("Open_vSwitch")
      iex> txn = OVSDB.Transaction.add(txn, OVSDB.Operation.comment("hello"))
      iex> txn.ops
      [%{"op" => "comment", "comment" => "hello"}]
  """
  @spec add(t(), Operation.t()) :: t()
  def add(%__MODULE__{ops: ops} = txn, operation) when is_map(operation) do
    %{txn | ops: ops ++ [operation]}
  end

  @doc """
  Prepends an operation to the front of the transaction. Rarely
  needed — use `add/2` in the overwhelming majority of cases.

      iex> txn = OVSDB.Transaction.new("Open_vSwitch")
      iex> txn = OVSDB.Transaction.add(txn, OVSDB.Operation.comment("second"))
      iex> txn = OVSDB.Transaction.prepend(txn, OVSDB.Operation.comment("first"))
      iex> Enum.map(txn.ops, & &1["comment"])
      ["first", "second"]
  """
  @spec prepend(t(), Operation.t()) :: t()
  def prepend(%__MODULE__{ops: ops} = txn, operation) when is_map(operation) do
    %{txn | ops: [operation | ops]}
  end

  @doc """
  Returns the number of operations currently in the transaction.

      iex> OVSDB.Transaction.new("db") |> OVSDB.Transaction.size()
      0
  """
  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{ops: ops}), do: length(ops)

  @doc """
  Returns `true` if the transaction has no operations. A transaction
  with no operations is technically valid per RFC 7047 but useless —
  it just returns an empty result list.

      iex> OVSDB.Transaction.new("db") |> OVSDB.Transaction.empty?()
      true
  """
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{ops: []}), do: true
  def empty?(%__MODULE__{}), do: false

  @doc """
  Converts the transaction to the params list for a `transact`
  request. The resulting list can be passed directly to
  `OVSDB.Protocol.request/3` as the second argument.

      iex> alias OVSDB.{Transaction, Operation}
      iex> txn =
      ...>   Transaction.new("Open_vSwitch")
      ...>   |> Transaction.add(Operation.comment("hello"))
      iex> Transaction.to_params(txn)
      ["Open_vSwitch", %{"op" => "comment", "comment" => "hello"}]
  """
  @spec to_params(t()) :: [String.t() | Operation.t(), ...]
  def to_params(%__MODULE__{db: db, ops: ops}) do
    [db | ops]
  end

  @doc """
  Convenience: build a complete `transact` request in one call.

  Returns the JSON-RPC request map ready for wire serialization.

      iex> alias OVSDB.{Transaction, Operation}
      iex> txn =
      ...>   Transaction.new("Open_vSwitch")
      ...>   |> Transaction.add(Operation.comment("hello"))
      iex> OVSDB.Transaction.to_request(txn, 42)
      %{
        "method" => "transact",
        "params" => ["Open_vSwitch", %{"op" => "comment", "comment" => "hello"}],
        "id" => 42
      }
  """
  @spec to_request(t(), OVSDB.Protocol.id()) :: OVSDB.Protocol.request()
  def to_request(%__MODULE__{} = txn, request_id) do
    OVSDB.Protocol.request("transact", to_params(txn), request_id)
  end
end
