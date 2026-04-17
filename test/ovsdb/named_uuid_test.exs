defmodule OVSDB.NamedUUIDTest do
  use ExUnit.Case, async: true
  doctest OVSDB.NamedUUID

  alias OVSDB.NamedUUID

  describe "new/1" do
    test "creates a NamedUUID from a valid identifier" do
      assert %NamedUUID{name: "new_bridge"} = NamedUUID.new("new_bridge")
    end

    test "accepts identifiers with digits after first char" do
      assert %NamedUUID{name: "abc123"} = NamedUUID.new("abc123")
    end

    test "accepts single-character identifiers that aren't underscore" do
      assert %NamedUUID{name: "x"} = NamedUUID.new("x")
      assert %NamedUUID{name: "X"} = NamedUUID.new("X")
      assert %NamedUUID{name: "a"} = NamedUUID.new("a")
    end

    test "rejects bare underscore (reserved prefix)" do
      assert_raise ArgumentError, ~r/reserved/, fn -> NamedUUID.new("_") end
    end

    test "raises on identifiers starting with underscore (reserved)" do
      assert_raise ArgumentError, ~r/reserved/, fn ->
        NamedUUID.new("_reserved")
      end
    end

    test "raises on identifiers starting with a digit" do
      assert_raise ArgumentError, fn -> NamedUUID.new("1starts_with_digit") end
    end

    test "raises on identifiers with whitespace or special chars" do
      for bad <- ["has spaces", "has-dash", "has.dot", "has/slash", ""] do
        assert_raise ArgumentError, fn -> NamedUUID.new(bad) end
      end
    end
  end

  describe "encode/1 and decode/1" do
    test "round-trip preserves name" do
      n = NamedUUID.new("my_symbol")
      assert {:ok, ^n} = NamedUUID.decode(NamedUUID.encode(n))
    end

    test "encode produces RFC 7047 wire form" do
      assert NamedUUID.encode(NamedUUID.new("my_sym")) == ["named-uuid", "my_sym"]
    end

    test "decode rejects reserved identifiers" do
      assert {:error, :invalid_named_uuid} = NamedUUID.decode(["named-uuid", "_reserved"])
    end

    test "decode rejects invalid identifier formats" do
      assert {:error, :invalid_named_uuid} = NamedUUID.decode(["named-uuid", "1bad"])
      assert {:error, :invalid_named_uuid} = NamedUUID.decode(["named-uuid", "has spaces"])
    end

    test "decode rejects wrong wire form" do
      assert {:error, :malformed} = NamedUUID.decode(["uuid", "foo"])
      assert {:error, :malformed} = NamedUUID.decode("not an array")
      assert {:error, :malformed} = NamedUUID.decode(%{})
    end
  end
end
