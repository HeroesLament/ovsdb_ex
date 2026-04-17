defmodule OVSDB.UUID do
  @moduledoc """
  An OVSDB UUID — a persistent reference to a row.

  ## Wire form

  Per [RFC 7047 §5.1][1], a UUID is encoded on the wire as a 2-element
  JSON array:

      ["uuid", "550e8400-e29b-41d4-a716-446655440000"]

  The second element is the 36-character canonical RFC 4122 string
  form: 32 lowercase hex digits grouped `8-4-4-4-12` and joined by
  hyphens.

  [1]: https://www.rfc-editor.org/rfc/rfc7047#section-5.1

  ## Elixir representation

  UUIDs must be wrapped in a struct because a bare string is
  indistinguishable from a non-UUID string at the type level:

      iex> OVSDB.UUID.new("550e8400-e29b-41d4-a716-446655440000")
      %OVSDB.UUID{value: "550e8400-e29b-41d4-a716-446655440000"}

  ## Generation

  `generate/0` produces a random v4 UUID using `:crypto.strong_rand_bytes/1`.
  This is the only version OVSDB servers produce for `_uuid` columns,
  and it's what callers should use when building insert operations
  that need real (not `named-uuid`) references.

  ## Why not the `Elixir.UUID` package?

  The `uuid` hex package is not a hard dependency of this library. We
  implement the narrow subset of RFC 4122 that OVSDB uses — v4
  generation, string parse/format, validation — in a few dozen lines
  with no external deps. Callers who prefer the `uuid` package can
  construct UUIDs via `new/1` from any 36-char string.
  """

  @enforce_keys [:value]
  defstruct [:value]

  @type t :: %__MODULE__{value: String.t()}

  # 8-4-4-4-12 hex with hyphens. Lowercase per RFC 4122 §3.
  @regex ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/

  @doc """
  Creates a UUID from its canonical string form.

  Raises `ArgumentError` if the string is not a valid 36-character
  RFC 4122 representation.

      iex> OVSDB.UUID.new("550e8400-e29b-41d4-a716-446655440000")
      %OVSDB.UUID{value: "550e8400-e29b-41d4-a716-446655440000"}

      iex> OVSDB.UUID.new("not-a-uuid")
      ** (ArgumentError) invalid UUID string: "not-a-uuid"
  """
  @spec new(String.t()) :: t()
  def new(string) when is_binary(string) do
    case parse(string) do
      {:ok, uuid} -> uuid
      {:error, _} -> raise ArgumentError, ~s(invalid UUID string: #{inspect(string)})
    end
  end

  @doc """
  Creates a UUID from its canonical string form, returning a result tuple.

      iex> OVSDB.UUID.parse("550e8400-e29b-41d4-a716-446655440000")
      {:ok, %OVSDB.UUID{value: "550e8400-e29b-41d4-a716-446655440000"}}

      iex> OVSDB.UUID.parse("nope")
      {:error, :invalid_uuid}

  Uppercase hex is accepted and normalized to lowercase, matching the
  canonical RFC 4122 form.
  """
  @spec parse(String.t()) :: {:ok, t()} | {:error, :invalid_uuid}
  def parse(string) when is_binary(string) do
    downcased = String.downcase(string)

    if Regex.match?(@regex, downcased) do
      {:ok, %__MODULE__{value: downcased}}
    else
      {:error, :invalid_uuid}
    end
  end

  def parse(_), do: {:error, :invalid_uuid}

  @doc """
  Generates a random RFC 4122 v4 UUID.

  Version and variant bits are set per RFC 4122 §4.4:

    * version (bits 48-51, i.e. high nibble of octet 6) = `0b0100` (4)
    * variant (bits 64-65, i.e. high 2 bits of octet 8) = `0b10`
      (which makes the high nibble of octet 8 one of `8`, `9`, `a`, `b`)

  The remaining 122 bits come from `:crypto.strong_rand_bytes/1`.

  ## Bit layout

      time_low (32) | time_mid (16) | time_hi_and_version (16) |
      clock_seq_hi_and_reserved (8) | clock_seq_low (8) | node (48)

  See RFC 4122 §4.1.2.
  """
  @spec generate() :: t()
  def generate do
    <<time_low::32, time_mid::16, _v::4, time_hi::12, _r::2, clock_seq::14, node::48>> =
      :crypto.strong_rand_bytes(16)

    # Insert version (4) in place of _v and variant (0b10) in place of _r.
    formatted =
      :io_lib.format(
        ~c"~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
        [
          time_low,
          time_mid,
          Bitwise.bor(0x4000, time_hi),
          Bitwise.bor(0x8000, clock_seq),
          node
        ]
      )
      |> :erlang.iolist_to_binary()

    %__MODULE__{value: formatted}
  end

  @doc """
  Encodes a UUID to its RFC 7047 wire form.

      iex> uuid = OVSDB.UUID.new("550e8400-e29b-41d4-a716-446655440000")
      iex> OVSDB.UUID.encode(uuid)
      ["uuid", "550e8400-e29b-41d4-a716-446655440000"]
  """
  @spec encode(t()) :: [String.t(), ...]
  def encode(%__MODULE__{value: value}), do: ["uuid", value]

  @doc """
  Decodes a UUID from its RFC 7047 wire form.

      iex> OVSDB.UUID.decode(["uuid", "550e8400-e29b-41d4-a716-446655440000"])
      {:ok, %OVSDB.UUID{value: "550e8400-e29b-41d4-a716-446655440000"}}

      iex> OVSDB.UUID.decode(["uuid", "garbage"])
      {:error, :invalid_uuid}

      iex> OVSDB.UUID.decode("not a uuid wire form")
      {:error, :malformed}
  """
  @spec decode(term()) :: {:ok, t()} | {:error, :invalid_uuid | :malformed}
  def decode(["uuid", value]) when is_binary(value), do: parse(value)
  def decode(_), do: {:error, :malformed}
end
