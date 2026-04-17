defmodule OVSDB.UUIDTest do
  use ExUnit.Case, async: true

  alias OVSDB.UUID

  describe "generate/0" do
    test "produces a struct with a valid uuid string" do
      uuid = UUID.generate()
      assert %UUID{value: v} = uuid
      assert is_binary(v)
      assert match?({:ok, _}, UUID.parse(v))
    end

    test "produces unique uuids" do
      uuids = for _ <- 1..100, do: UUID.generate().value
      assert Enum.uniq(uuids) == uuids
    end

    test "sets version 4 and RFC 4122 variant bits" do
      # Version is the first nibble of the 3rd group (char at index 14).
      # Variant is the first two bits of the 4th group (char at index 19):
      # per RFC 4122 §4.1.1 the high bits must be 10, so the first hex
      # digit of the 4th group is one of 8, 9, a, b.
      for _ <- 1..50 do
        %UUID{value: v} = UUID.generate()
        assert String.at(v, 14) == "4"
        variant_char = String.at(v, 19)
        assert variant_char in ["8", "9", "a", "b"]
      end
    end
  end

  describe "new/1" do
    test "wraps a valid uuid string" do
      s = "550e8400-e29b-41d4-a716-446655440000"
      assert UUID.new(s) == %UUID{value: s}
    end

    test "normalizes case" do
      upper = "550E8400-E29B-41D4-A716-446655440000"
      assert %UUID{value: "550e8400-e29b-41d4-a716-446655440000"} = UUID.new(upper)
    end

    test "raises on invalid input" do
      assert_raise ArgumentError, fn -> UUID.new("not-a-uuid") end
      assert_raise ArgumentError, fn -> UUID.new("") end
    end
  end

  describe "parse/1" do
    test "accepts a valid uuid string" do
      s = "550e8400-e29b-41d4-a716-446655440000"
      assert {:ok, %UUID{value: ^s}} = UUID.parse(s)
    end

    test "rejects malformed input" do
      assert {:error, :invalid_uuid} = UUID.parse("not-a-uuid")
      assert {:error, :invalid_uuid} = UUID.parse("550e8400-e29b-41d4-a716")
      assert {:error, :invalid_uuid} = UUID.parse("")
    end

    test "rejects non-binary input" do
      assert {:error, :invalid_uuid} = UUID.parse(42)
      assert {:error, :invalid_uuid} = UUID.parse(nil)
    end

    test "downcases uppercase input" do
      upper = "550E8400-E29B-41D4-A716-446655440000"
      lower = "550e8400-e29b-41d4-a716-446655440000"
      assert {:ok, %UUID{value: ^lower}} = UUID.parse(upper)
    end
  end

  describe "encode/1" do
    test "produces [\"uuid\", string] wire form" do
      u = UUID.new("550e8400-e29b-41d4-a716-446655440000")
      assert UUID.encode(u) == ["uuid", "550e8400-e29b-41d4-a716-446655440000"]
    end

    test "round-trips via decode" do
      original = UUID.generate()
      wire = UUID.encode(original)
      assert {:ok, decoded} = UUID.decode(wire)
      assert decoded == original
    end
  end

  describe "decode/1" do
    test "decodes [\"uuid\", s] wire form" do
      s = "550e8400-e29b-41d4-a716-446655440000"
      assert {:ok, %UUID{value: ^s}} = UUID.decode(["uuid", s])
    end

    test "rejects other shapes" do
      assert {:error, :malformed} = UUID.decode("raw-string")
      assert {:error, :malformed} = UUID.decode(["named-uuid", "foo"])
      assert {:error, :malformed} = UUID.decode(["uuid"])
      assert {:error, :malformed} = UUID.decode(42)
    end

    test "propagates invalid uuid error from parse" do
      assert {:error, :invalid_uuid} = UUID.decode(["uuid", "not-a-uuid"])
    end
  end
end
