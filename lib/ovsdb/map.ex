defmodule OVSDB.Map do
  @moduledoc """
  An OVSDB map — a collection of key/value pairs, both of atomic type.

  ## Wire form

  Per [RFC 7047 §5.1][1], a map is always encoded on the wire as a
  2-element tagged JSON array:

      ["map", [[k1, v1], [k2, v2], ...]]

  Unlike `OVSDB.Set`, there is no bare-value optimization for
  maps — even a 1-element map is fully tagged. Decoding is therefore
  unambiguous.

  [1]: https://www.rfc-editor.org/rfc/rfc7047#section-5.1

  ## Name collision

  This module shadows Elixir's built-in `Map`. Inside this file we
  refer to the standard library as `Elixir.Map` when needed. Callers
  who `alias OVSDB.Map` should either use the full module
  name or alias as `alias OVSDB.Map, as: OvsdbMap`.

  ## Ordering

  OVSDB maps are logically unordered. We store entries as a list of
  2-tuples `[{k, v}, ...]` rather than as a native Elixir `%{}` for
  three reasons:

    1. It preserves the wire's encoding structure for lossless
       roundtrips.
    2. It supports non-string atomic keys (integers, booleans) which
       Elixir maps handle but JSON wire form constrains to atomic
       types only.
    3. It avoids the mental tax of "is this an OVSDB map or an
       Elixir map" at the struct level.

  Use `equal?/2` for logical (order-independent) equality, and
  `to_elixir_map/1` when you want a native `%{}` for ergonomic access.
  """

  @enforce_keys [:entries]
  defstruct [:entries]

  @type t :: %__MODULE__{entries: [{term(), term()}]}

  @typedoc """
  A map that is statically known to be empty. Narrower than `t()` so
  that Dialyzer can verify `empty/0`'s return type exactly.
  """
  @type empty :: %__MODULE__{entries: []}

  @doc """
  Creates a map from a list of `{key, value}` tuples or from a native
  Elixir map.

      iex> OVSDB.Map.new([{"a", 1}, {"b", 2}])
      %OVSDB.Map{entries: [{"a", 1}, {"b", 2}]}

      iex> OVSDB.Map.new(%{"a" => 1, "b" => 2}) |> OVSDB.Map.equal?(
      ...>   OVSDB.Map.new([{"a", 1}, {"b", 2}])
      ...> )
      true
  """
  @spec new([{term(), term()}] | %{optional(term()) => term()}) :: t()
  def new(entries) when is_list(entries) do
    Enum.each(entries, fn
      {_k, _v} -> :ok
      other -> raise ArgumentError, "map entries must be {key, value} tuples, got: #{inspect(other)}"
    end)

    %__MODULE__{entries: entries}
  end

  def new(%{} = map) do
    %__MODULE__{entries: Elixir.Map.to_list(map)}
  end

  @doc """
  Returns the empty map.
  """
  @spec empty() :: empty()
  def empty, do: %__MODULE__{entries: []}

  @doc """
  Returns the number of entries in the map.
  """
  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{entries: entries}), do: length(entries)

  @doc """
  Returns the value associated with `key`, or `default` if not present.
  O(n) — maps are stored as lists, not hash tables.

      iex> m = OVSDB.Map.new([{"a", 1}, {"b", 2}])
      iex> OVSDB.Map.get(m, "a")
      1
      iex> OVSDB.Map.get(m, "missing", :not_found)
      :not_found
  """
  @spec get(t(), term(), term()) :: term()
  def get(%__MODULE__{entries: entries}, key, default \\ nil) do
    case List.keyfind(entries, key, 0) do
      {^key, value} -> value
      nil -> default
    end
  end

  @doc """
  Returns `true` if the two maps contain the same entries regardless
  of order.

      iex> a = OVSDB.Map.new([{"a", 1}, {"b", 2}])
      iex> b = OVSDB.Map.new([{"b", 2}, {"a", 1}])
      iex> OVSDB.Map.equal?(a, b)
      true
  """
  @spec equal?(t(), t()) :: boolean()
  def equal?(%__MODULE__{entries: a}, %__MODULE__{entries: b}) do
    Enum.sort(a) == Enum.sort(b)
  end

  @doc """
  Converts to a native Elixir map. Loses original entry ordering.
  Raises on duplicate keys (which shouldn't occur in valid OVSDB
  maps — the server enforces key uniqueness).

      iex> m = OVSDB.Map.new([{"a", 1}, {"b", 2}])
      iex> OVSDB.Map.to_elixir_map(m)
      %{"a" => 1, "b" => 2}
  """
  @spec to_elixir_map(t()) :: %{optional(term()) => term()}
  def to_elixir_map(%__MODULE__{entries: entries}) do
    Elixir.Map.new(entries)
  end

  @doc """
  Encodes a map to its RFC 7047 wire form.

  As with `Set`, nested wrapped types (UUID, NamedUUID) must already
  be encoded by the caller — this function does not recurse.

      iex> m = OVSDB.Map.new([{"k1", "v1"}, {"k2", "v2"}])
      iex> OVSDB.Map.encode(m)
      ["map", [["k1", "v1"], ["k2", "v2"]]]

      iex> OVSDB.Map.encode(OVSDB.Map.empty())
      ["map", []]
  """
  @typedoc """
  The wire form of a map — always a tagged `["map", [[k, v], ...]]`
  array, regardless of cardinality.
  """
  @type wire :: nonempty_list()

  @spec encode(t()) :: wire()
  def encode(%__MODULE__{entries: entries}) do
    ["map", Enum.map(entries, fn {k, v} -> [k, v] end)]
  end

  @doc """
  Decodes the `["map", [...]]` wire form.

      iex> OVSDB.Map.decode(["map", [["k1", "v1"], ["k2", "v2"]]])
      {:ok, %OVSDB.Map{entries: [{"k1", "v1"}, {"k2", "v2"}]}}

      iex> OVSDB.Map.decode(["map", []])
      {:ok, %OVSDB.Map{entries: []}}

      iex> OVSDB.Map.decode(["not-map", []])
      {:error, :malformed}
  """
  @spec decode(term()) :: {:ok, t()} | {:error, :malformed}
  def decode(["map", entries]) when is_list(entries) do
    with {:ok, tuples} <- decode_entries(entries, []) do
      {:ok, %__MODULE__{entries: tuples}}
    end
  end

  def decode(_), do: {:error, :malformed}

  @doc """
  Decodes the wire form with per-key and per-value decoder callbacks.
  Used by the IDL layer to resolve nested UUIDs in ref-typed columns.

      iex> uuid_wire = ["uuid", "550e8400-e29b-41d4-a716-446655440000"]
      iex> wire = ["map", [["k", uuid_wire]]]
      iex> value_decoder = fn w -> {:ok, u} = OVSDB.UUID.decode(w); u end
      iex> OVSDB.Map.decode_with(wire, &Function.identity/1, value_decoder)
      {:ok, %OVSDB.Map{entries: [{"k", %OVSDB.UUID{value: "550e8400-e29b-41d4-a716-446655440000"}}]}}
  """
  @spec decode_with(term(), (term() -> term()), (term() -> term())) ::
          {:ok, t()} | {:error, :malformed}
  def decode_with(["map", entries], key_decoder, value_decoder) when is_list(entries) do
    decoded =
      Enum.map(entries, fn
        [k, v] -> {key_decoder.(k), value_decoder.(v)}
        _ -> throw(:malformed_entry)
      end)

    {:ok, %__MODULE__{entries: decoded}}
  catch
    :malformed_entry -> {:error, :malformed}
  end

  def decode_with(_, _, _), do: {:error, :malformed}

  # --- private ---

  defp decode_entries([], acc), do: {:ok, Enum.reverse(acc)}

  defp decode_entries([[k, v] | rest], acc) do
    decode_entries(rest, [{k, v} | acc])
  end

  defp decode_entries(_, _), do: {:error, :malformed}
end
