defmodule OVSDB.Framer do
  @moduledoc """
  Incremental JSON message framer for the OVSDB wire protocol.

  OVSDB sends bare JSON objects back-to-back on the stream with
  **no length prefix and no delimiter** between messages — per
  [RFC 7047 §4][rfc-wire] (which defers to JSON-RPC 1.0 for framing,
  which itself says nothing). Receivers find message boundaries by
  brace-counting at the JSON structural level.

  [rfc-wire]: https://www.rfc-editor.org/rfc/rfc7047#section-4

  ## What this module does

  Accumulates bytes from the wire and extracts complete JSON objects
  as they become available. The state machine tracks:

    * Depth of `{` vs `}` — incremented and decremented only outside
      of strings, so `"{"` inside a string literal doesn't affect
      nesting.
    * Whether we're currently inside a JSON string.
    * Whether the previous character was `\\\\` (escape), so that
      `"\\\""` inside a string doesn't terminate the string.

  When depth returns to zero after incrementing, one complete
  message has been framed and is returned as a binary slice of the
  buffer.

  ## Why hand-written and not Jason

  Jason doesn't expose an incremental decoder. The alternatives
  (re-parse the entire buffer on every chunk and catch partial-input
  errors) are quadratic in message size and have ambiguous
  "incomplete vs malformed" signaling. The hand-written state
  machine is ~50 lines, zero-dep, and matches what every other
  OVSDB implementation does.

  ## UTF-8 safety

  RFC 8259 (JSON) mandates UTF-8 source and restricts structural
  characters (`{`, `}`, `"`, `[`, `]`, `,`, `:`) to their ASCII
  code points. Multi-byte UTF-8 sequences within strings never
  contain ASCII bytes, so byte-level brace counting is safe.

  ## Usage

      framer = Framer.new()

      # Feed bytes as they arrive from :gen_tcp
      {framer, messages} = Framer.feed(framer, chunk_1)
      {framer, more}     = Framer.feed(framer, chunk_2)

      # `messages` is a list of complete JSON binaries, ready for Jason.decode/1
      for msg <- messages, do: handle(Jason.decode!(msg))

  The framer never drops or reorders bytes; partial messages are
  buffered until completion. Malformed input (e.g. unbalanced
  braces at top level, bytes before the first `{`) is detected and
  returned as an error.
  """

  @enforce_keys []
  defstruct buffer: "", depth: 0, in_string: false, escape: false, start: 0, offset: 0

  @type t :: %__MODULE__{
          buffer: binary(),
          depth: non_neg_integer(),
          in_string: boolean(),
          escape: boolean(),
          start: non_neg_integer(),
          offset: non_neg_integer()
        }

  @typedoc """
  A freshly-created framer. Narrower than `t()` — all fields are at
  their initial values.
  """
  @type empty :: %__MODULE__{
          buffer: <<>>,
          depth: 0,
          in_string: false,
          escape: false,
          start: 0,
          offset: 0
        }

  @type framer_error :: :unexpected_byte | :buffer_overflow

  @doc """
  Creates a new empty framer.

      iex> framer = OVSDB.Framer.new()
      iex> framer.depth
      0
      iex> framer.buffer
      ""
  """
  @spec new() :: empty()
  def new, do: %__MODULE__{}

  @doc """
  Feeds a chunk of bytes into the framer.

  Returns `{framer, messages}` where `messages` is a list of
  complete JSON object binaries (in order), or
  `{:error, framer_error(), partial_framer}` on malformed input.

  Incomplete messages are held in the framer's internal buffer and
  emitted on subsequent `feed/2` calls once their closing `}`
  arrives.

      iex> {f, msgs} = OVSDB.Framer.feed(OVSDB.Framer.new(), ~s({"a":1}))
      iex> msgs
      [~s({"a":1})]
      iex> f.buffer
      ""

      iex> {f, msgs} = OVSDB.Framer.feed(OVSDB.Framer.new(), ~s({"a":1}{"b":2}))
      iex> msgs
      [~s({"a":1}), ~s({"b":2})]
      iex> f.buffer
      ""

      iex> f = OVSDB.Framer.new()
      iex> {f, msgs} = OVSDB.Framer.feed(f, ~s({"a":))
      iex> msgs
      []
      iex> {_f, msgs} = OVSDB.Framer.feed(f, ~s(1}))
      iex> msgs
      [~s({"a":1})]
  """
  @spec feed(t(), binary()) ::
          {t(), [binary()]} | {:error, framer_error(), t()}
  def feed(%__MODULE__{} = framer, chunk) when is_binary(chunk) do
    new_buffer = framer.buffer <> chunk
    do_feed(%{framer | buffer: new_buffer}, [])
  end

  # ---------------------------------------------------------------------------
  # Private — the state machine
  # ---------------------------------------------------------------------------

  defp do_feed(%__MODULE__{buffer: buf, offset: off} = framer, acc)
       when byte_size(buf) == off do
    # All bytes consumed; compact the buffer by discarding prefix up
    # to `start`. In the steady state `start == off` after a
    # complete-message emission, so this is a cheap slice.
    remaining =
      if framer.start == byte_size(buf) do
        ""
      else
        binary_part(buf, framer.start, byte_size(buf) - framer.start)
      end

    new_offset = framer.offset - framer.start
    new_start = 0

    {%{framer | buffer: remaining, offset: new_offset, start: new_start}, Enum.reverse(acc)}
  end

  defp do_feed(%__MODULE__{} = framer, acc) do
    <<_::binary-size(framer.offset), byte, _::binary>> = framer.buffer
    handle_byte(framer, byte, acc)
  end

  # Inside a string, previous byte was a backslash → consume
  # unconditionally.
  defp handle_byte(%__MODULE__{in_string: true, escape: true} = framer, _byte, acc) do
    do_feed(%{framer | escape: false, offset: framer.offset + 1}, acc)
  end

  # Inside a string. Closing quote or escape start?
  defp handle_byte(%__MODULE__{in_string: true} = framer, byte, acc) do
    case byte do
      ?\\ -> do_feed(%{framer | escape: true, offset: framer.offset + 1}, acc)
      ?" -> do_feed(%{framer | in_string: false, offset: framer.offset + 1}, acc)
      _ -> do_feed(%{framer | offset: framer.offset + 1}, acc)
    end
  end

  # Outside a string at depth 0, waiting for a new message. Skip
  # interstitial whitespace; anything other than `{` or whitespace
  # is malformed.
  defp handle_byte(%__MODULE__{depth: 0} = framer, byte, acc) do
    cond do
      byte == ?{ ->
        framer = %{framer | depth: 1, start: framer.offset, offset: framer.offset + 1}
        do_feed(framer, acc)

      byte in [?\s, ?\t, ?\n, ?\r] ->
        framer = %{framer | offset: framer.offset + 1, start: framer.offset + 1}
        do_feed(framer, acc)

      true ->
        {:error, :unexpected_byte, framer}
    end
  end

  # Outside a string, inside a message (depth > 0). Track braces
  # and string openings.
  defp handle_byte(%__MODULE__{} = framer, byte, acc) do
    case byte do
      ?{ ->
        do_feed(%{framer | depth: framer.depth + 1, offset: framer.offset + 1}, acc)

      ?} ->
        new_depth = framer.depth - 1
        new_offset = framer.offset + 1

        if new_depth == 0 do
          # Message complete — slice [start, new_offset) out of buffer.
          len = new_offset - framer.start
          message = binary_part(framer.buffer, framer.start, len)

          framer = %{
            framer
            | depth: 0,
              offset: new_offset,
              start: new_offset
          }

          do_feed(framer, [message | acc])
        else
          do_feed(%{framer | depth: new_depth, offset: new_offset}, acc)
        end

      ?" ->
        do_feed(%{framer | in_string: true, offset: framer.offset + 1}, acc)

      _ ->
        do_feed(%{framer | offset: framer.offset + 1}, acc)
    end
  end
end
