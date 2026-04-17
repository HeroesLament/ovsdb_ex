defmodule OVSDB.Condition do
  @moduledoc """
  Builders for WHERE-clause predicates used in OVSDB operations
  (`select`, `update`, `delete`, `mutate`, `wait`).

  ## RFC 7047 §5.1

  A condition is a 3-element JSON array:

      [<column>, <function>, <value>]

  where `<function>` is one of the seven strings:

      "==" | "!=" | "<" | "<=" | ">" | ">=" | "includes" | "excludes"

  The semantics of `includes`/`excludes` depend on the column's type:

    * For scalar columns, they behave the same as `==`/`!=`.
    * For set columns, `includes` is set containment (every element in
      the value must be in the column); `excludes` is its complement.
    * For map columns, the value is a set of keys and the check is
      against the map's key set.

  ## Elixir form

  We carry conditions as tagged 3-tuples `{column, op, value}` where
  `op` is one of the atoms `:eq`, `:ne`, `:lt`, `:le`, `:gt`, `:ge`,
  `:includes`, `:excludes`. This keeps Elixir call sites readable
  while preserving the full RFC set:

      Condition.eq("serial_number", "SIM-DEADBEEF")
      Condition.includes("ports", OVSDB.Set.new([port_uuid]))

  The builders do NOT validate that the value matches the column's
  type — that requires schema knowledge and is `OVSDB.Schema`'s job.
  Builders only enforce well-formedness of the tuple shape.

  ## Encoding

  `encode/1` walks the value through `OVSDB.Value.encode/1` so that
  wrapped types (UUID, NamedUUID, Set, Map) produce correct RFC 7047
  wire form. Atomic values pass through unchanged.
  """

  alias OVSDB.Value

  @type op :: :eq | :ne | :lt | :le | :gt | :ge | :includes | :excludes

  @type t :: {column :: String.t(), op(), value :: Value.value()}

  @type wire :: [String.t() | Value.wire(), ...]

  # Map of atom operators to their RFC 7047 wire strings. Single
  # source of truth; used by both encode and by per-operator builders.
  @operators %{
    eq: "==",
    ne: "!=",
    lt: "<",
    le: "<=",
    gt: ">",
    ge: ">=",
    includes: "includes",
    excludes: "excludes"
  }

  @doc """
  Returns the list of atom operators this module supports.

      iex> OVSDB.Condition.operators() |> Enum.sort()
      [:eq, :excludes, :ge, :gt, :includes, :le, :lt, :ne]
  """
  @spec operators() :: [op()]
  def operators, do: Elixir.Map.keys(@operators)

  @doc """
  Returns the RFC 7047 wire string for an operator atom.

      iex> OVSDB.Condition.operator_string(:eq)
      "=="

      iex> OVSDB.Condition.operator_string(:includes)
      "includes"
  """
  @spec operator_string(op()) :: String.t()
  def operator_string(op) when is_atom(op) do
    case Elixir.Map.fetch(@operators, op) do
      {:ok, s} -> s
      :error -> raise ArgumentError, "unknown condition operator: #{inspect(op)}"
    end
  end

  # Per-operator builders. Generated from @operators so there's exactly
  # one place to edit if RFC 7047bis ever adds a function.
  for {atom, _str} <- @operators do
    @doc """
    Builds a `#{atom}` condition.

        iex> OVSDB.Condition.#{atom}("col", 42)
        {"col", :#{atom}, 42}
    """
    @spec unquote(atom)(String.t(), Value.value()) :: t()
    def unquote(atom)(column, value) when is_binary(column) do
      {column, unquote(atom), value}
    end
  end

  @doc """
  Builds a condition from an explicit column/op/value triple. Useful
  for programmatic construction where the operator is a variable.

      iex> OVSDB.Condition.new("col", :eq, 42)
      {"col", :eq, 42}

      iex> OVSDB.Condition.new("col", :bogus, 42)
      ** (ArgumentError) unknown condition operator: :bogus
  """
  @spec new(String.t(), op(), Value.value()) :: t()
  def new(column, op, value) when is_binary(column) and is_atom(op) do
    # Validate op by running it through operator_string/1, which
    # raises on unknown operators.
    _ = operator_string(op)
    {column, op, value}
  end

  @doc """
  Encodes a condition to its RFC 7047 wire form.

  The value is walked through `OVSDB.Value.encode/1` so that wrapped
  types produce correct wire form.

      iex> c = OVSDB.Condition.eq("name", "br-lan")
      iex> OVSDB.Condition.encode(c)
      ["name", "==", "br-lan"]

      iex> uuid = OVSDB.UUID.new("550e8400-e29b-41d4-a716-446655440000")
      iex> c = OVSDB.Condition.eq("_uuid", uuid)
      iex> OVSDB.Condition.encode(c)
      ["_uuid", "==", ["uuid", "550e8400-e29b-41d4-a716-446655440000"]]

      iex> set = OVSDB.Set.new([1, 2, 3])
      iex> c = OVSDB.Condition.includes("ports", set)
      iex> OVSDB.Condition.encode(c)
      ["ports", "includes", ["set", [1, 2, 3]]]
  """
  @spec encode(t()) :: wire()
  def encode({column, op, value}) when is_binary(column) and is_atom(op) do
    [column, operator_string(op), Value.encode(value)]
  end

  @doc """
  Encodes a list of conditions. Convenience for operation builders
  that accept a `where` clause.

      iex> conditions = [
      ...>   OVSDB.Condition.eq("col_a", 1),
      ...>   OVSDB.Condition.ne("col_b", "x")
      ...> ]
      iex> OVSDB.Condition.encode_all(conditions)
      [["col_a", "==", 1], ["col_b", "!=", "x"]]

      iex> OVSDB.Condition.encode_all([])
      []
  """
  @spec encode_all([t()]) :: [wire()]
  def encode_all(conditions) when is_list(conditions) do
    Enum.map(conditions, &encode/1)
  end
end
