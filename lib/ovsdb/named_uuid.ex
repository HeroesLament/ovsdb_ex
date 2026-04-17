defmodule OVSDB.NamedUUID do
  @moduledoc """
  A symbolic UUID used inside a `transact` request to reference a row
  that is being inserted in the same transaction.

  ## Wire form

  Per [RFC 7047 §5.1][1], a named UUID is encoded as a 2-element JSON
  array:

      ["named-uuid", "my_bridge"]

  The second element is an `<id>` — matching `[a-zA-Z_][a-zA-Z0-9_]*`.

  [1]: https://www.rfc-editor.org/rfc/rfc7047#section-5.1

  ## Use case

  When inserting a row, the server assigns it a real UUID. But within
  the same transaction, *other* operations may need to reference that
  row before the server has responded. Named UUIDs bridge this gap:

      # Insert a bridge and a port that references it, in one transaction
      [
        Operation.insert("Bridge", %{"name" => "br-lan"}, uuid_name: "new_br"),
        Operation.insert("Port", %{"name" => "eth0", "bridge" => NamedUUID.new("new_br")})
      ]

  The server resolves `"new_br"` to the actual UUID it assigned to the
  inserted Bridge row, substituting it everywhere the name appears in
  the transaction.

  ## Scope

  Named UUIDs are *only* valid within a single transaction. They have
  no meaning outside a `transact` request. After the transaction
  commits, references must use real `OVSDB.UUID` values.
  """

  @enforce_keys [:name]
  defstruct [:name]

  @type t :: %__MODULE__{name: String.t()}

  # RFC 7047 §3.1: <id> matches [a-zA-Z_][a-zA-Z0-9_]*
  @regex ~r/\A[a-zA-Z_][a-zA-Z0-9_]*\z/

  @doc """
  Creates a named UUID from an identifier string.

  Raises `ArgumentError` if the name does not match the RFC 7047 `<id>`
  production (`[a-zA-Z_][a-zA-Z0-9_]*`).

  Names starting with `_` are reserved to the implementation per §3.1
  and will raise.

      iex> OVSDB.NamedUUID.new("new_bridge")
      %OVSDB.NamedUUID{name: "new_bridge"}

      iex> OVSDB.NamedUUID.new("1starts_with_digit")
      ** (ArgumentError) invalid named-uuid identifier: "1starts_with_digit"

      iex> OVSDB.NamedUUID.new("_reserved")
      ** (ArgumentError) named-uuid identifiers beginning with "_" are reserved, got: "_reserved"
  """
  @spec new(String.t()) :: t()
  def new("_" <> _ = name) do
    raise ArgumentError,
          ~s(named-uuid identifiers beginning with "_" are reserved, got: #{inspect(name)})
  end

  def new(name) when is_binary(name) do
    if Regex.match?(@regex, name) do
      %__MODULE__{name: name}
    else
      raise ArgumentError, ~s(invalid named-uuid identifier: #{inspect(name)})
    end
  end

  @doc """
  Encodes a named UUID to its RFC 7047 wire form.

      iex> OVSDB.NamedUUID.new("new_br") |> OVSDB.NamedUUID.encode()
      ["named-uuid", "new_br"]
  """
  @spec encode(t()) :: [String.t(), ...]
  def encode(%__MODULE__{name: name}), do: ["named-uuid", name]

  @doc """
  Decodes a named UUID from its RFC 7047 wire form.

      iex> OVSDB.NamedUUID.decode(["named-uuid", "new_br"])
      {:ok, %OVSDB.NamedUUID{name: "new_br"}}

      iex> OVSDB.NamedUUID.decode(["named-uuid", "_reserved"])
      {:error, :invalid_named_uuid}
  """
  @spec decode(term()) :: {:ok, t()} | {:error, :invalid_named_uuid | :malformed}
  def decode(["named-uuid", name]) when is_binary(name) do
    {:ok, new(name)}
  rescue
    ArgumentError -> {:error, :invalid_named_uuid}
  end

  def decode(_), do: {:error, :malformed}
end
