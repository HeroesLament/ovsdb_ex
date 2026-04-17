defmodule OVSDB.NamedUUIDTest do
  use ExUnit.Case, async: true

  alias OVSDB.NamedUUID

  describe "new/1" do
    test "accepts valid identifiers" do
      assert NamedUUID.new("br_int") == %NamedUUID{name: "br_int"}
      assert NamedUUID.new("abc123") == %NamedUUID{name: "abc123"}
      assert NamedUUID.new("x") == %NamedUUID{name: "x"}
    end

    test "rejects identifiers starting with underscore (reserved)" do
      assert_raise ArgumentError, ~r/reserved/, fn ->
        NamedUUID.new("_reserved")
      end
    end

    test "rejects identifiers starting with a digit" do
      assert_raise ArgumentError, ~r/invalid/, fn ->
        NamedUUID.new("1foo")
      end
    end

    test "rejects empty string" do
      assert_raise ArgumentError, ~r/invalid/, fn ->
        NamedUUID.new("")
      end
    end

    test "rejects identifiers with special characters" do
      assert_raise ArgumentError, ~r/invalid/, fn -> NamedUUID.new("has-dash") end
      assert_raise ArgumentError, ~r/invalid/, fn -> NamedUUID.new("has space") end
      assert_raise ArgumentError, ~r/invalid/, fn -> NamedUUID.new("has.dot") end
    end
  end

  describe "encode/1" do
    test "produces [\"named-uuid\", name] wire form" do
      n = NamedUUID.new("br_int")
      assert NamedUUID.encode(n) == ["named-uuid", "br_int"]
    end
  end

  describe "decode/1" do
    test "decodes the wire form" do
      assert {:ok, %NamedUUID{name: "br_int"}} =
               NamedUUID.decode(["named-uuid", "br_int"])
    end

    test "rejects reserved names" do
      assert {:error, :invalid_named_uuid} =
               NamedUUID.decode(["named-uuid", "_reserved"])
    end

    test "rejects malformed envelopes" do
      assert {:error, :malformed} = NamedUUID.decode(["uuid", "foo"])
      assert {:error, :malformed} = NamedUUID.decode(["named-uuid"])
      assert {:error, :malformed} = NamedUUID.decode("just a string")
    end

    test "round-trips via encode" do
      original = NamedUUID.new("new_bridge")
      wire = NamedUUID.encode(original)
      assert {:ok, ^original} = NamedUUID.decode(wire)
    end
  end
end
