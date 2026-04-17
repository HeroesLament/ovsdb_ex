defmodule OVSDB.ValueTest do
  use ExUnit.Case, async: true

  alias OVSDB.{Map, NamedUUID, Set, UUID, Value}

  describe "encode/1 — atomic types" do
    test "integers pass through unchanged" do
      assert Value.encode(42) == 42
      assert Value.encode(-1) == -1
      assert Value.encode(0) == 0
    end

    test "floats pass through unchanged" do
      assert Value.encode(3.14) == 3.14
      assert Value.encode(-0.5) == -0.5
    end

    test "strings pass through unchanged" do
      assert Value.encode("hello") == "hello"
      assert Value.encode("") == ""
    end

    test "booleans pass through unchanged" do
      assert Value.encode(true) == true
      assert Value.encode(false) == false
    end
  end

  describe "encode/1 — rejects invalid atoms" do
    test "raises on nil" do
      assert_raise ArgumentError, ~r/nil/, fn -> Value.encode(nil) end
    end

    test "raises on arbitrary atoms" do
      assert_raise ArgumentError, fn -> Value.encode(:some_atom) end
    end

    test "raises on tuples, pids, refs, etc." do
      assert_raise ArgumentError, fn -> Value.encode({1, 2}) end
      assert_raise ArgumentError, fn -> Value.encode(make_ref()) end
    end
  end

  describe "encode/1 — UUID and NamedUUID" do
    test "UUID struct encodes to wire form" do
      u = UUID.new("550e8400-e29b-41d4-a716-446655440000")
      assert Value.encode(u) == ["uuid", "550e8400-e29b-41d4-a716-446655440000"]
    end

    test "NamedUUID struct encodes to wire form" do
      n = NamedUUID.new("br_int")
      assert Value.encode(n) == ["named-uuid", "br_int"]
    end
  end

  describe "encode/1 — Set" do
    test "empty set" do
      assert Value.encode(Set.empty()) == ["set", []]
    end

    test "1-element set optimizes to bare value" do
      assert Value.encode(Set.new([42])) == 42
    end

    test "multi-element set uses tagged form" do
      assert Value.encode(Set.new([1, 2, 3])) == ["set", [1, 2, 3]]
    end

    test "recursively encodes UUID elements" do
      u1 = UUID.new("11111111-1111-1111-1111-111111111111")
      u2 = UUID.new("22222222-2222-2222-2222-222222222222")

      assert Value.encode(Set.new([u1, u2])) == [
               "set",
               [
                 ["uuid", "11111111-1111-1111-1111-111111111111"],
                 ["uuid", "22222222-2222-2222-2222-222222222222"]
               ]
             ]
    end

    test "recursively encodes a UUID in 1-element set" do
      u = UUID.new("550e8400-e29b-41d4-a716-446655440000")
      assert Value.encode(Set.new([u])) == ["uuid", "550e8400-e29b-41d4-a716-446655440000"]
    end
  end

  describe "encode/1 — Map" do
    test "empty map" do
      assert Value.encode(Map.empty()) == ["map", []]
    end

    test "simple string map" do
      m = Map.new([{"k1", "v1"}, {"k2", "v2"}])
      assert Value.encode(m) == ["map", [["k1", "v1"], ["k2", "v2"]]]
    end

    test "recursively encodes UUID values" do
      u = UUID.new("550e8400-e29b-41d4-a716-446655440000")
      m = Map.new([{"ref", u}])

      assert Value.encode(m) == [
               "map",
               [["ref", ["uuid", "550e8400-e29b-41d4-a716-446655440000"]]]
             ]
    end
  end

  describe "decode_atom/1" do
    test "decodes bare atomic values" do
      assert {:ok, 42} = Value.decode_atom(42)
      assert {:ok, 3.14} = Value.decode_atom(3.14)
      assert {:ok, "str"} = Value.decode_atom("str")
      assert {:ok, true} = Value.decode_atom(true)
      assert {:ok, false} = Value.decode_atom(false)
    end

    test "decodes uuid wire form" do
      s = "550e8400-e29b-41d4-a716-446655440000"
      assert {:ok, %UUID{value: ^s}} = Value.decode_atom(["uuid", s])
    end

    test "decodes named-uuid wire form" do
      assert {:ok, %NamedUUID{name: "foo"}} = Value.decode_atom(["named-uuid", "foo"])
    end

    test "rejects set and map forms (use decode_value)" do
      assert {:error, _} = Value.decode_atom(["set", [1, 2]])
      assert {:error, _} = Value.decode_atom(["map", []])
    end
  end

  describe "decode_value/1" do
    test "decodes bare atomic values" do
      assert {:ok, 42} = Value.decode_value(42)
      assert {:ok, "str"} = Value.decode_value("str")
    end

    test "decodes tagged set form" do
      assert {:ok, %Set{elements: [1, 2, 3]}} = Value.decode_value(["set", [1, 2, 3]])
    end

    test "decodes tagged map form" do
      assert {:ok, %Map{entries: [{"k", "v"}]}} =
               Value.decode_value(["map", [["k", "v"]]])
    end

    test "recursively decodes UUID elements in a set" do
      wire = ["set", [["uuid", "11111111-1111-1111-1111-111111111111"]]]
      assert {:ok, %Set{elements: [%UUID{}]}} = Value.decode_value(wire)
    end

    test "round-trips via encode for simple types" do
      values = [
        42,
        "str",
        true,
        Set.new([1, 2, 3]),
        Map.new([{"a", 1}])
      ]

      for v <- values do
        wire = Value.encode(v)
        assert {:ok, decoded} = Value.decode_value(wire)
        assert decoded == v
      end
    end
  end
end
