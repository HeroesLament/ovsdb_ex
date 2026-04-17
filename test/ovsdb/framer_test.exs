defmodule OVSDB.FramerTest do
  use ExUnit.Case, async: true

  alias OVSDB.Framer

  describe "new/0" do
    test "creates an empty framer" do
      f = Framer.new()
      assert f.buffer == ""
      assert f.depth == 0
      refute f.in_string
      refute f.escape
      assert f.offset == 0
      assert f.start == 0
    end
  end

  describe "feed/2 — single complete messages" do
    test "frames a single simple message" do
      {f, msgs} = Framer.feed(Framer.new(), ~s({"a":1}))
      assert msgs == [~s({"a":1})]
      assert f.buffer == ""
      assert f.depth == 0
    end

    test "frames a message with nested objects" do
      {_f, msgs} = Framer.feed(Framer.new(), ~s({"a":{"b":1}}))
      assert msgs == [~s({"a":{"b":1}})]
    end

    test "frames a message with deep nesting" do
      input = ~s({"a":{"b":{"c":{"d":1}}}})
      {_f, msgs} = Framer.feed(Framer.new(), input)
      assert msgs == [input]
    end

    test "frames a message containing arrays" do
      input = ~s({"a":[1,2,3]})
      {_f, msgs} = Framer.feed(Framer.new(), input)
      assert msgs == [input]
    end
  end

  describe "feed/2 — multiple messages in one chunk" do
    test "frames two back-to-back messages" do
      {_f, msgs} = Framer.feed(Framer.new(), ~s({"a":1}{"b":2}))
      assert msgs == [~s({"a":1}), ~s({"b":2})]
    end

    test "frames many back-to-back messages" do
      chunks = for i <- 1..5, into: "", do: ~s({"i":#{i}})
      {_f, msgs} = Framer.feed(Framer.new(), chunks)
      assert length(msgs) == 5
      assert Enum.at(msgs, 0) == ~s({"i":1})
      assert Enum.at(msgs, 4) == ~s({"i":5})
    end
  end

  describe "feed/2 — split messages" do
    test "completes a message split across two feeds" do
      {f, msgs1} = Framer.feed(Framer.new(), ~s({"a":))
      assert msgs1 == []
      assert f.depth == 1

      {f, msgs2} = Framer.feed(f, ~s(1}))
      assert msgs2 == [~s({"a":1})]
      assert f.depth == 0
    end

    test "completes message split at every position" do
      # The canonical stress test: split this message at every byte
      # boundary and verify we always get back the original.
      msg = ~s({"a":1,"b":"str","c":[1,2,3]})

      for split <- 0..byte_size(msg) do
        a = binary_part(msg, 0, split)
        b = binary_part(msg, split, byte_size(msg) - split)

        {f, msgs1} = Framer.feed(Framer.new(), a)
        {_f, msgs2} = Framer.feed(f, b)

        combined = msgs1 ++ msgs2

        assert combined == [msg],
               "split at #{split} failed: expected [#{msg}], got #{inspect(combined)}"
      end
    end

    test "handles three-way split" do
      msg = ~s({"key":"value","n":42})

      {f1, msgs1} = Framer.feed(Framer.new(), binary_part(msg, 0, 5))
      {f2, msgs2} = Framer.feed(f1, binary_part(msg, 5, 10))
      {_f3, msgs3} = Framer.feed(f2, binary_part(msg, 15, byte_size(msg) - 15))

      assert msgs1 ++ msgs2 ++ msgs3 == [msg]
    end
  end

  describe "feed/2 — string handling" do
    test "does not split on braces inside strings" do
      input = ~s({"key":"has}brace"})
      {_f, msgs} = Framer.feed(Framer.new(), input)
      assert msgs == [input]
    end

    test "does not split on open braces inside strings" do
      input = ~s({"key":"has{brace"})
      {_f, msgs} = Framer.feed(Framer.new(), input)
      assert msgs == [input]
    end

    test "handles escaped quotes in strings" do
      input = ~s({"key":"has\\"quote"})
      {_f, msgs} = Framer.feed(Framer.new(), input)
      assert msgs == [input]
    end

    test "handles escaped backslashes before quotes" do
      # \\ followed by " is actually an escaped backslash then a
      # closing quote — the string ends here
      input = ~s({"key":"path\\\\"})
      {_f, msgs} = Framer.feed(Framer.new(), input)
      assert msgs == [input]
    end

    test "handles escape split across feeds" do
      # Split at the backslash so the \" spans chunks
      {f, msgs1} = Framer.feed(Framer.new(), ~s({"k":"a\\))
      assert msgs1 == []

      {_f, msgs2} = Framer.feed(f, ~s("b"}))
      assert msgs2 == [~s({"k":"a\\"b"})]
    end
  end

  describe "feed/2 — arrays" do
    test "depth tracking works with nested arrays" do
      input = ~s({"a":[[1,2],[3,4]]})
      {_f, msgs} = Framer.feed(Framer.new(), input)
      assert msgs == [input]
    end
  end

  describe "feed/2 — UTF-8 safety" do
    test "frames message with UTF-8 content" do
      input = ~s({"name":"café"})
      {_f, msgs} = Framer.feed(Framer.new(), input)
      assert msgs == [input]
    end

    test "frames message with emoji" do
      input = ~s({"emoji":"🎉"})
      {_f, msgs} = Framer.feed(Framer.new(), input)
      assert msgs == [input]
    end

    test "handles UTF-8 multi-byte split across feeds" do
      # "é" is 2 bytes in UTF-8: <<0xc3, 0xa9>>
      msg = ~s({"name":"café"})

      for split <- 0..byte_size(msg) do
        a = binary_part(msg, 0, split)
        b = binary_part(msg, split, byte_size(msg) - split)

        {f, msgs1} = Framer.feed(Framer.new(), a)
        {_f, msgs2} = Framer.feed(f, b)

        assert msgs1 ++ msgs2 == [msg],
               "UTF-8 split at #{split} failed"
      end
    end
  end

  describe "feed/2 — preserves buffer on incomplete input" do
    test "buffer contains pending bytes" do
      {f, []} = Framer.feed(Framer.new(), ~s({"incomplete))
      assert f.buffer == ~s({"incomplete)
      assert f.depth == 1
      assert f.in_string == true
    end

    test "buffer is cleared after complete message" do
      {f, _} = Framer.feed(Framer.new(), ~s({"a":1}))
      assert f.buffer == ""
      assert f.offset == 0
      assert f.start == 0
    end

    test "buffer retains only the incomplete tail after partial completion" do
      # Complete one message, start a second
      {f, msgs} = Framer.feed(Framer.new(), ~s({"a":1}{"b":2))
      assert msgs == [~s({"a":1})]
      assert f.buffer == ~s({"b":2)
      assert f.depth == 1
    end
  end

  describe "feed/2 — empty input" do
    test "empty binary produces no messages and unchanged framer state" do
      f = Framer.new()
      {f2, msgs} = Framer.feed(f, "")
      assert msgs == []
      assert f2.buffer == f.buffer
      assert f2.depth == f.depth
    end
  end

  describe "feed/2 — stress" do
    test "frames 100 messages fed byte-by-byte" do
      messages = for i <- 1..100, do: ~s({"id":#{i},"method":"echo"})
      concatenated = Enum.join(messages)

      # Feed one byte at a time
      {final_framer, all_msgs} =
        Enum.reduce(
          0..(byte_size(concatenated) - 1),
          {Framer.new(), []},
          fn i, {f, acc} ->
            byte = binary_part(concatenated, i, 1)
            {new_f, new_msgs} = Framer.feed(f, byte)
            {new_f, acc ++ new_msgs}
          end
        )

      assert final_framer.buffer == ""
      assert final_framer.depth == 0
      assert all_msgs == messages
    end
  end
end
