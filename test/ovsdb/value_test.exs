defmodule OVSDB.ValueTest do
  use ExUnit.Case, async: true
  doctest OVSDB.Value

  alias OVSDB.{Value, UUID, NamedUUID, Set, Map}

  describe "encode/1 — atomic types pass through" do
    test "integers" do
      assert Value.encode(42) == 42
      assert Value.encode(-7) == -7
      assert Value.encode(0) == 0
    end

    test "reals" do
      assert Value.encode(3.14) == 3.14
      assert Value.encode(0.0) == 0.0
    end

    test "booleans" do
      assert Value.encode(true) == true
      assert Value.encode(false) == false
    end

    test "strings" do
      assert Value.encode("hello") == "hello"
      assert Value.encode("") == ""
      assert Value.encode("unicode: ñ") == "unicode: ñ"
    end
  end

  describe "encode/1 — wrapped types" do
    test "UUID encodes to tagged wire form" do
      u = UUID.new("550e8400-e29b-41d4-a716-446655440000")
      assert Value.encode(u) == ["uuid", "550e8400-e29b-41d4-a716-446655440000"]
    end

    test "NamedUUID encodes to tagged wire form" do
      n = NamedUUID.new("new_br")
      assert Value.encode(n) == ["named-uuid", "new_br"]
    end
  end

  describe "encode/1 — Sets" do
    test "empty set" do
      assert Value.encode(Set.empty()) == ["set", []]
    end

    test "multi-element set with atomic values" do
      assert Value.encode(Set.new([1, 2, 3])) == ["set", [1, 2, 3]]
    end

    test "one-element set of atomic value (bare optimization)" do
      assert Value.encode(Set.new(["only"])) == "only"
    end

    test "recursively encodes nested UUIDs in multi-element set" do
      u1 = UUID.new("11111111-1111-4111-8111-111111111111")
      u2 = UUID.new("22222222-2222-4222-8222-222222222222")

      assert Value.encode(Set.new([u1, u2])) == [
               "set",
               [
                 ["uuid", "11111111-1111-4111-8111-111111111111"],
                 ["uuid", "22222222-2222-4222-8222-222222222222"]
               ]
             ]
    end

    test "one-element set of UUID (bare tagged-array)" do
      u = UUID.new("11111111-1111-4111-8111-111111111111")
      assert Value.encode(Set.new([u])) == ["uuid", "11111111-1111-4111-8111-111111111111"]
    end
  end

  describe "encode/1 — Maps" do
    test "empty map" do
      assert Value.encode(Map.empty()) == ["map", []]
    end

    test "map with atomic values" do
      assert Value.encode(Map.new([{"k", "v"}])) == ["map", [["k", "v"]]]
    end

    test "recursively encodes UUID values in maps" do
      u = UUID.new("11111111-1111-4111-8111-111111111111")

      assert Value.encode(Map.new([{"port", u}])) == [
               "map",
               [["port", ["uuid", "11111111-1111-4111-8111-111111111111"]]]
             ]
    end

    test "recursively encodes NamedUUID values in maps" do
      n = NamedUUID.new("new_port")

      assert Value.encode(Map.new([{"port", n}])) == [
               "map",
               [["port", ["named-uuid", "new_port"]]]
             ]
    end
  end

  describe "encode/1 — errors" do
    test "raises on nil" do
      assert_raise ArgumentError, ~r/nil/, fn -> Value.encode(nil) end
    end

    test "raises on arbitrary atoms" do
      assert_raise ArgumentError, fn -> Value.encode(:some_atom) end
    end

    test "raises on tuples" do
      assert_raise ArgumentError, fn -> Value.encode({1, 2}) end
    end

    test "raises on native Elixir maps (must be wrapped)" do
      assert_raise ArgumentError, fn -> Value.encode(%{foo: :bar}) end
    end
  end

  describe "decode_atom/1" do
    test "accepts all atomic types" do
      assert {:ok, 42} = Value.decode_atom(42)
      assert {:ok, 3.14} = Value.decode_atom(3.14)
      assert {:ok, true} = Value.decode_atom(true)
      assert {:ok, false} = Value.decode_atom(false)
      assert {:ok, "s"} = Value.decode_atom("s")
    end

    test "decodes tagged uuid wire form" do
      assert {:ok, %UUID{value: "550e8400-e29b-41d4-a716-446655440000"}} =
               Value.decode_atom(["uuid", "550e8400-e29b-41d4-a716-446655440000"])
    end

    test "decodes tagged named-uuid wire form" do
      assert {:ok, %NamedUUID{name: "new_br"}} = Value.decode_atom(["named-uuid", "new_br"])
    end

    test "rejects composite forms" do
      assert {:error, {:not_an_atom, _}} = Value.decode_atom(["set", []])
      assert {:error, {:not_an_atom, _}} = Value.decode_atom(["map", []])
    end

    test "rejects non-atom terms" do
      assert {:error, {:not_an_atom, _}} = Value.decode_atom(%{})
      assert {:error, {:not_an_atom, _}} = Value.decode_atom({1, 2})
    end
  end

  describe "decode_value/1 — tagged forms" do
    test "decodes bare integers" do
      assert {:ok, 42} = Value.decode_value(42)
    end

    test "decodes bare strings" do
      assert {:ok, "hello"} = Value.decode_value("hello")
    end

    test "decodes UUID wire form" do
      assert {:ok, %UUID{}} = Value.decode_value(["uuid", "550e8400-e29b-41d4-a716-446655440000"])
    end

    test "decodes NamedUUID wire form" do
      assert {:ok, %NamedUUID{}} = Value.decode_value(["named-uuid", "new_br"])
    end

    test "decodes Set wire form" do
      assert {:ok, %Set{elements: [1, 2, 3]}} = Value.decode_value(["set", [1, 2, 3]])
    end

    test "decodes Map wire form" do
      assert {:ok, %Map{entries: [{"k", "v"}]}} = Value.decode_value(["map", [["k", "v"]]])
    end

    test "recursively decodes UUIDs nested in Sets" do
      wire = [
        "set",
        [
          ["uuid", "11111111-1111-4111-8111-111111111111"],
          ["uuid", "22222222-2222-4222-8222-222222222222"]
        ]
      ]

      {:ok, %Set{elements: [u1, u2]}} = Value.decode_value(wire)
      assert %UUID{} = u1
      assert %UUID{} = u2
    end

    test "recursively decodes UUIDs nested in Map values" do
      wire = [
        "map",
        [["key", ["uuid", "11111111-1111-4111-8111-111111111111"]]]
      ]

      {:ok, %Map{entries: [{"key", uuid}]}} = Value.decode_value(wire)
      assert %UUID{} = uuid
    end
  end

  describe "encode/decode round-trips" do
    test "round-trips all fully-tagged forms" do
      for value <- [
            42,
            3.14,
            true,
            false,
            "a string",
            UUID.new("abcdef01-2345-4678-8abc-def012345678"),
            NamedUUID.new("my_symbol"),
            Set.new([]),
            Set.new([1, 2, 3]),
            Set.new([
              UUID.new("abcdef01-2345-4678-8abc-def012345678"),
              UUID.new("11111111-1111-4111-8111-111111111111")
            ]),
            Map.new([]),
            Map.new([{"k", "v"}, {"k2", 42}])
          ] do
        encoded = Value.encode(value)
        {:ok, decoded} = Value.decode_value(encoded)

        matches =
          cond do
            is_struct(value, Set) -> Set.equal?(value, decoded)
            is_struct(value, Map) -> Map.equal?(value, decoded)
            true -> value == decoded
          end

        assert matches,
               "round-trip mismatch for #{inspect(value)}: decoded to #{inspect(decoded)}"
      end
    end

    # The documented lossy case: a 1-element set of one atomic value
    # encodes to bare form, and the schema-blind decoder returns that
    # bare value, NOT a %Set{} wrapper. This is expected.
    test "1-element set of atomic does NOT round-trip via decode_value (documented lossy case)" do
      s = Set.new([42])
      assert 42 = Value.encode(s)
      # Without schema, we can't recover the Set wrapper
      assert {:ok, 42} = Value.decode_value(42)
    end
  end
end
