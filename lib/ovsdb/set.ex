defmodule OVSDB.Set do
  @moduledoc """
  An OVSDB set — an unordered collection of atomic values.

  ## Wire form

  Per [RFC 7047 §5.1][1], a set is encoded on the wire in one of two
  forms depending on cardinality:

    * **Exactly one element**: encoded as the bare element value, *not*
      as a tagged array. This is an optimization for the common case
      where a column of set-type has exactly one member.

    * **Zero or 2+ elements**: encoded as a 2-element tagged JSON
      array: `["set", [e1, e2, ...]]`.

  [1]: https://www.rfc-editor.org/rfc/rfc7047#section-5.1

  ## Decoding ambiguity

  Because a 1-element set is encoded as the bare value, a decoder
  cannot determine whether a bare value like `42` should become
  `%Set{elements: [42]}` or stay as `42` without knowing the column's
  schema type. This library resolves the ambiguity by providing two
  decode entry points:

    * `decode_tagged/1` — decodes only the explicit `["set", [...]]`
      form. Returns `:error` on bare values. Use this when you already
      know the value is a set.

    * `decode_for_column/2` — schema-aware decoder used by the IDL
      layer. Treats a bare value as a 1-element set when the column's
      schema says the column is of set-type.

  For the symmetric encode/decode roundtrip, `encode/1` is always
  lossless but `decode_tagged/1` only recovers the 0-or-2+-element
  cases — which is exactly what you want when the wire form came from
  an unambiguous non-set context.

  ## Equality

  Sets are unordered — `%Set{elements: [1, 2]}` and `%Set{elements:
  [2, 1]}` represent the same logical value. Struct equality via `==`
  compares the lists in order and will report them as unequal. Use
  `equal?/2` for logical set equality.
  """

  @enforce_keys [:elements]
  defstruct [:elements]

  @type t :: %__MODULE__{elements: [term()]}

  @doc """
  Creates a set from a list of elements.

  Duplicates in the input are *not* removed — OVSDB treats sets as
  logically deduplicated, but the wire form is a list and the server
  is expected to reject sets containing duplicates (RFC 7047 §5.1
  says a set's elements must be unique of the specified type).
  Callers who want deduplication should do it explicitly via
  `Enum.uniq/1` before construction.

      iex> OVSDB.Set.new([1, 2, 3])
      %OVSDB.Set{elements: [1, 2, 3]}

      iex> OVSDB.Set.new([])
      %OVSDB.Set{elements: []}
  """
  @spec new([term()]) :: t()
  def new(elements) when is_list(elements), do: %__MODULE__{elements: elements}

  @doc """
  Returns the empty set.

      iex> OVSDB.Set.empty()
      %OVSDB.Set{elements: []}
  """
  @spec empty() :: t()
  def empty, do: %__MODULE__{elements: []}

  @doc """
  Returns the number of elements in the set.
  """
  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{elements: elements}), do: length(elements)

  @doc """
  Returns `true` if the two sets contain the same elements
  regardless of order.

      iex> a = OVSDB.Set.new([1, 2, 3])
      iex> b = OVSDB.Set.new([3, 1, 2])
      iex> OVSDB.Set.equal?(a, b)
      true
  """
  @spec equal?(t(), t()) :: boolean()
  def equal?(%__MODULE__{elements: a}, %__MODULE__{elements: b}) do
    Enum.sort(a) == Enum.sort(b)
  end

  @doc """
  Encodes a set to its RFC 7047 wire form.

  A 1-element set is encoded as the bare element; all other sets are
  encoded as `["set", [...]]`. Nested `UUID`, `NamedUUID`, and other
  wrapped types must already be encoded to their wire form by the
  caller — this function does not recurse into elements.

      iex> s = OVSDB.Set.new([1, 2, 3])
      iex> OVSDB.Set.encode(s)
      ["set", [1, 2, 3]]

      iex> s = OVSDB.Set.new(["single"])
      iex> OVSDB.Set.encode(s)
      "single"

      iex> OVSDB.Set.encode(OVSDB.Set.empty())
      ["set", []]
  """
  @spec encode(t()) :: term()
  def encode(%__MODULE__{elements: [single]}), do: single
  def encode(%__MODULE__{elements: elements}), do: ["set", elements]

  @doc """
  Decodes the explicit `["set", [...]]` wire form. Does *not* accept
  bare values — use `decode_for_column/2` for schema-aware decoding.

      iex> OVSDB.Set.decode_tagged(["set", [1, 2, 3]])
      {:ok, %OVSDB.Set{elements: [1, 2, 3]}}

      iex> OVSDB.Set.decode_tagged(["set", []])
      {:ok, %OVSDB.Set{elements: []}}

      iex> OVSDB.Set.decode_tagged("single")
      {:error, :malformed}
  """
  @spec decode_tagged(term()) :: {:ok, t()} | {:error, :malformed}
  def decode_tagged(["set", elements]) when is_list(elements) do
    {:ok, new(elements)}
  end

  def decode_tagged(_), do: {:error, :malformed}

  @doc """
  Decodes a wire value known-from-context to be of set type.

  Accepts both the explicit `["set", [...]]` form and the bare-value
  1-element optimization. The `element_decoder` callback is applied
  to each element; pass `&Function.identity/1` to keep raw values.

      iex> OVSDB.Set.decode_for_column(["set", [1, 2]], &Function.identity/1)
      {:ok, %OVSDB.Set{elements: [1, 2]}}

      iex> OVSDB.Set.decode_for_column("lone", &Function.identity/1)
      {:ok, %OVSDB.Set{elements: ["lone"]}}

      iex> OVSDB.Set.decode_for_column(
      ...>   ["set", [["uuid", "550e8400-e29b-41d4-a716-446655440000"]]],
      ...>   fn wire -> {:ok, u} = OVSDB.UUID.decode(wire); u end
      ...> )
      {:ok, %OVSDB.Set{elements: [%OVSDB.UUID{value: "550e8400-e29b-41d4-a716-446655440000"}]}}
  """
  @spec decode_for_column(term(), (term() -> term())) :: {:ok, t()} | {:error, term()}
  def decode_for_column(["set", elements], element_decoder) when is_list(elements) do
    {:ok, new(Enum.map(elements, element_decoder))}
  end

  def decode_for_column(bare, element_decoder) do
    {:ok, new([element_decoder.(bare)])}
  end
end
