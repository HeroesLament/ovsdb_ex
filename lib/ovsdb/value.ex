defmodule OVSDB.Value do
  @moduledoc """
  Dispatcher for encoding and decoding any OVSDB `<value>`.

  Per [RFC 7047 §5.1][1], a `<value>` is one of:

    * `<atom>` — an atomic value: integer, real, boolean, string, UUID,
      or named UUID.
    * `<set>` — an `OVSDB.Set`.
    * `<map>` — an `OVSDB.Map`.

  [1]: https://www.rfc-editor.org/rfc/rfc7047#section-5.1

  ## Encoding

  `encode/1` walks any Elixir term representing an OVSDB value and
  produces the JSON-ready wire form:

    * Native `integer`, `float`, `boolean`, `binary` → pass through
      as-is (JSON handles them).
    * `%UUID{}` / `%NamedUUID{}` / `%Set{}` / `%Map{}` → delegates to
      the type's `encode/1`, recursively encoding any nested values.

  Encoding is unambiguous — every Elixir term has exactly one wire
  representation.

  ## Decoding

  Decoding is NOT unambiguous in isolation. A wire value of `42`
  could be:

    * A bare integer (atomic type)
    * A 1-element set of integers (`Set.new([42])`)

  The schema disambiguates. Layer 1 therefore provides three decode
  entry points:

    * `decode_atom/1` — decodes a wire value that is known to be
      atomic. Recognizes `["uuid", _]` and `["named-uuid", _]` tagged
      forms; everything else is returned as a bare Elixir value.

    * `decode_value/1` — decodes a wire value WITHOUT schema context,
      returning whatever can be determined unambiguously. Tagged
      forms (`["set", _]`, `["map", _]`, `["uuid", _]`, `["named-uuid",
      _]`) decode to their structs; bare values pass through as-is.
      This is what monitor notifications use — the server always
      sends the fully-tagged form except for the 1-element-set
      optimization, which a schema-blind decoder cannot recover.

    * `decode_for_type/2` — decodes with schema knowledge. Given a
      `type` descriptor (see `OVSDB.Type`), applies the
      correct strategy: atomic values pass through, set-typed values
      are always wrapped in `%Set{}` even when they appeared bare on
      the wire, and so on.

  The IDL layer uses `decode_for_type/2`. Protocol-level decoders for
  things that aren't row data (e.g. RPC result payloads) use
  `decode_value/1`.

  ## What is NOT handled here

  This module does no schema validation — values are decoded to their
  structural form without checking type constraints (enum membership,
  min/max bounds, refTable correctness, etc.). That is the job of
  `OVSDB.Schema.validate_row/3`.
  """

  alias OVSDB.{UUID, NamedUUID, Set, Map}

  @doc """
  Encodes an Elixir term to its OVSDB wire form.

  ## Examples

      iex> OVSDB.Value.encode(42)
      42

      iex> OVSDB.Value.encode("hello")
      "hello"

      iex> OVSDB.Value.encode(true)
      true

      iex> uuid = OVSDB.UUID.new("550e8400-e29b-41d4-a716-446655440000")
      iex> OVSDB.Value.encode(uuid)
      ["uuid", "550e8400-e29b-41d4-a716-446655440000"]

      iex> set = OVSDB.Set.new([1, 2, 3])
      iex> OVSDB.Value.encode(set)
      ["set", [1, 2, 3]]

      iex> set_with_uuids = OVSDB.Set.new([
      ...>   OVSDB.UUID.new("550e8400-e29b-41d4-a716-446655440000")
      ...> ])
      iex> OVSDB.Value.encode(set_with_uuids)
      ["uuid", "550e8400-e29b-41d4-a716-446655440000"]
  """
  @spec encode(term()) :: term()
  def encode(%UUID{} = u), do: UUID.encode(u)
  def encode(%NamedUUID{} = n), do: NamedUUID.encode(n)

  def encode(%Set{elements: elements}) do
    encoded = Enum.map(elements, &encode/1)
    Set.encode(%Set{elements: encoded})
  end

  def encode(%Map{entries: entries}) do
    encoded = Enum.map(entries, fn {k, v} -> {encode(k), encode(v)} end)
    Map.encode(%Map{entries: encoded})
  end

  def encode(atom) when is_atom(atom) do
    # Only true/false/nil are valid OVSDB atoms (bool + null). Other
    # atoms are a caller bug.
    case atom do
      true -> true
      false -> false
      nil -> raise ArgumentError, "nil is not a valid OVSDB value (OVSDB has no null type)"
      other -> raise ArgumentError, "atom #{inspect(other)} is not a valid OVSDB value"
    end
  end

  def encode(int) when is_integer(int), do: int
  def encode(real) when is_float(real), do: real
  def encode(bin) when is_binary(bin), do: bin

  def encode(other) do
    raise ArgumentError, "cannot encode #{inspect(other)} as an OVSDB value"
  end

  @doc """
  Decodes a wire value that is known to be atomic (not a set or map).
  Tagged `["uuid", _]` and `["named-uuid", _]` forms decode to their
  structs; everything else passes through.

      iex> OVSDB.Value.decode_atom(42)
      {:ok, 42}

      iex> OVSDB.Value.decode_atom("hello")
      {:ok, "hello"}

      iex> OVSDB.Value.decode_atom(["uuid", "550e8400-e29b-41d4-a716-446655440000"])
      {:ok, %OVSDB.UUID{value: "550e8400-e29b-41d4-a716-446655440000"}}

      iex> OVSDB.Value.decode_atom(["named-uuid", "new_br"])
      {:ok, %OVSDB.NamedUUID{name: "new_br"}}
  """
  @spec decode_atom(term()) :: {:ok, term()} | {:error, term()}
  def decode_atom(["uuid", _] = wire), do: UUID.decode(wire)
  def decode_atom(["named-uuid", _] = wire), do: NamedUUID.decode(wire)
  def decode_atom(int) when is_integer(int), do: {:ok, int}
  def decode_atom(real) when is_float(real), do: {:ok, real}
  def decode_atom(bool) when is_boolean(bool), do: {:ok, bool}
  def decode_atom(bin) when is_binary(bin), do: {:ok, bin}
  def decode_atom(other), do: {:error, {:not_an_atom, other}}

  @doc """
  Schema-blind decoder. Recognizes the four tagged forms (`uuid`,
  `named-uuid`, `set`, `map`) and decodes them to structs;
  everything else passes through as a bare atomic value.

  Because a 1-element set is encoded bare, this decoder cannot
  recover it — it will be returned as whatever atomic value was on
  the wire. Use `decode_for_type/2` with schema knowledge to recover
  the `%Set{}` wrapper.

      iex> OVSDB.Value.decode_value(42)
      {:ok, 42}

      iex> OVSDB.Value.decode_value(["set", [1, 2, 3]])
      {:ok, %OVSDB.Set{elements: [1, 2, 3]}}

      iex> OVSDB.Value.decode_value(["map", [["k", "v"]]])
      {:ok, %OVSDB.Map{entries: [{"k", "v"}]}}

      iex> OVSDB.Value.decode_value(["set", [["uuid", "550e8400-e29b-41d4-a716-446655440000"]]])
      {:ok, %OVSDB.Set{elements: [%OVSDB.UUID{value: "550e8400-e29b-41d4-a716-446655440000"}]}}
  """
  @spec decode_value(term()) :: {:ok, term()} | {:error, term()}
  def decode_value(["uuid", _] = wire), do: UUID.decode(wire)
  def decode_value(["named-uuid", _] = wire), do: NamedUUID.decode(wire)

  def decode_value(["set", elements] = wire) when is_list(elements) do
    Set.decode_for_column(wire, fn el ->
      case decode_value(el) do
        {:ok, v} -> v
        {:error, _} -> el
      end
    end)
  end

  def decode_value(["map", entries] = wire) when is_list(entries) do
    decode_fn = fn v ->
      case decode_value(v) do
        {:ok, decoded} -> decoded
        {:error, _} -> v
      end
    end

    Map.decode_with(wire, decode_fn, decode_fn)
  end

  def decode_value(bare), do: decode_atom(bare)
end
