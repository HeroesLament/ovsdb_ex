defmodule OVSDB.SetTest do
  use ExUnit.Case, async: true
  doctest OVSDB.Set

  alias OVSDB.{Set, UUID}

  describe "new/1 and empty/0" do
    test "creates a set from a list" do
      assert %Set{elements: [1, 2, 3]} = Set.new([1, 2, 3])
    end

    test "accepts an empty list" do
      assert %Set{elements: []} = Set.new([])
    end

    test "empty/0 returns the empty set" do
      assert %Set{elements: []} = Set.empty()
    end

    test "does not deduplicate (that's the caller's job)" do
      assert %Set{elements: [1, 1, 2]} = Set.new([1, 1, 2])
    end
  end

  describe "size/1" do
    test "returns the number of elements" do
      assert Set.size(Set.empty()) == 0
      assert Set.size(Set.new([1])) == 1
      assert Set.size(Set.new([1, 2, 3])) == 3
    end
  end

  describe "equal?/2" do
    test "returns true for same elements regardless of order" do
      assert Set.equal?(Set.new([1, 2, 3]), Set.new([3, 2, 1]))
    end

    test "returns true for both empty" do
      assert Set.equal?(Set.empty(), Set.empty())
    end

    test "returns false for different elements" do
      refute Set.equal?(Set.new([1, 2]), Set.new([1, 2, 3]))
    end

    test "works with struct-typed elements" do
      u1 = UUID.new("550e8400-e29b-41d4-a716-446655440000")
      u2 = UUID.new("11111111-2222-4333-8444-555555555555")
      assert Set.equal?(Set.new([u1, u2]), Set.new([u2, u1]))
    end
  end

  describe "encode/1 — RFC 7047 wire form" do
    test "empty set encodes as tagged empty array" do
      assert Set.encode(Set.empty()) == ["set", []]
    end

    test "multi-element set encodes as tagged array" do
      assert Set.encode(Set.new([1, 2, 3])) == ["set", [1, 2, 3]]
    end

    test "single-element set encodes as bare element (RFC 7047 optimization)" do
      assert Set.encode(Set.new(["only"])) == "only"
      assert Set.encode(Set.new([42])) == 42
      assert Set.encode(Set.new([true])) == true
    end
  end

  describe "decode_tagged/1 — strict" do
    test "decodes multi-element tagged arrays" do
      assert {:ok, %Set{elements: [1, 2, 3]}} = Set.decode_tagged(["set", [1, 2, 3]])
    end

    test "decodes empty tagged array" do
      assert {:ok, %Set{elements: []}} = Set.decode_tagged(["set", []])
    end

    test "rejects bare values" do
      assert {:error, :malformed} = Set.decode_tagged("single")
      assert {:error, :malformed} = Set.decode_tagged(42)
      assert {:error, :malformed} = Set.decode_tagged(nil)
    end

    test "rejects wrong tag" do
      assert {:error, :malformed} = Set.decode_tagged(["map", []])
      assert {:error, :malformed} = Set.decode_tagged(["uuid", "foo"])
    end

    test "rejects malformed structure" do
      assert {:error, :malformed} = Set.decode_tagged(["set"])
      assert {:error, :malformed} = Set.decode_tagged(["set", [], "extra"])
      assert {:error, :malformed} = Set.decode_tagged(["set", "not a list"])
    end
  end

  describe "decode_for_column/2 — schema-aware" do
    test "decodes tagged form" do
      assert {:ok, %Set{elements: [1, 2]}} =
               Set.decode_for_column(["set", [1, 2]], &Function.identity/1)
    end

    test "treats bare value as single-element set" do
      assert {:ok, %Set{elements: [42]}} = Set.decode_for_column(42, &Function.identity/1)
      assert {:ok, %Set{elements: ["x"]}} = Set.decode_for_column("x", &Function.identity/1)
    end

    test "applies element_decoder to each element" do
      uuid_wire = ["uuid", "550e8400-e29b-41d4-a716-446655440000"]

      decoder = fn wire ->
        {:ok, u} = UUID.decode(wire)
        u
      end

      {:ok, set} = Set.decode_for_column(["set", [uuid_wire, uuid_wire]], decoder)
      assert length(set.elements) == 2
      assert %UUID{} = hd(set.elements)
    end

    test "applies element_decoder to bare single value" do
      uuid_wire = ["uuid", "550e8400-e29b-41d4-a716-446655440000"]

      decoder = fn wire ->
        {:ok, u} = UUID.decode(wire)
        u
      end

      {:ok, %Set{elements: [uuid]}} = Set.decode_for_column(uuid_wire, decoder)
      assert %UUID{} = uuid
    end
  end

  describe "encode/decode round-trips" do
    test "all non-one-element cardinalities round-trip via decode_for_column" do
      for elements <- [[], [1, 2], [1, 2, 3, 4, 5]] do
        s = Set.new(elements)
        {:ok, recovered} = Set.decode_for_column(Set.encode(s), &Function.identity/1)
        assert Set.equal?(s, recovered)
      end
    end

    test "one-element case round-trips via decode_for_column (schema-aware)" do
      s = Set.new([42])
      {:ok, recovered} = Set.decode_for_column(Set.encode(s), &Function.identity/1)
      assert Set.equal?(s, recovered)
    end

    test "one-element case does NOT round-trip via decode_tagged (schema-blind)" do
      s = Set.new([42])
      # encode produces bare 42
      assert 42 = Set.encode(s)
      # decode_tagged rejects bare values
      assert {:error, :malformed} = Set.decode_tagged(42)
    end
  end
end
