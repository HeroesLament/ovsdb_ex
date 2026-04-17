defmodule OVSDB.SetTest do
  use ExUnit.Case, async: true

  alias OVSDB.Set

  describe "new/1" do
    test "wraps a list of elements" do
      assert Set.new([1, 2, 3]) == %Set{elements: [1, 2, 3]}
    end

    test "accepts empty list" do
      assert Set.new([]) == %Set{elements: []}
    end
  end

  describe "empty/0" do
    test "returns a set with no elements" do
      assert Set.empty() == %Set{elements: []}
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
    test "treats sets with same elements in any order as equal" do
      assert Set.equal?(Set.new([1, 2, 3]), Set.new([3, 2, 1]))
    end

    test "distinguishes sets with different elements" do
      refute Set.equal?(Set.new([1, 2]), Set.new([1, 2, 3]))
      refute Set.equal?(Set.new([1, 2]), Set.new([3, 4]))
    end

    test "treats empty sets as equal" do
      assert Set.equal?(Set.empty(), Set.empty())
      assert Set.equal?(Set.empty(), Set.new([]))
    end
  end

  describe "encode/1 — wire form optimization" do
    # RFC 7047 §5.1: a 1-element set is encoded as the bare element;
    # only empty and 2+ element sets use the ["set", [...]] form.

    test "1-element set encodes to bare element" do
      assert Set.encode(Set.new(["only"])) == "only"
      assert Set.encode(Set.new([42])) == 42
    end

    test "empty set encodes to [\"set\", []]" do
      assert Set.encode(Set.empty()) == ["set", []]
    end

    test "multi-element set encodes to [\"set\", [...]]" do
      assert Set.encode(Set.new([1, 2, 3])) == ["set", [1, 2, 3]]
    end

    test "preserves element order" do
      assert Set.encode(Set.new(["a", "b", "c"])) == ["set", ["a", "b", "c"]]
    end
  end

  describe "decode_tagged/1" do
    test "accepts the tagged wire form" do
      assert {:ok, %Set{elements: [1, 2, 3]}} = Set.decode_tagged(["set", [1, 2, 3]])
    end

    test "accepts the empty tagged form" do
      assert {:ok, %Set{elements: []}} = Set.decode_tagged(["set", []])
    end

    test "rejects bare values (use decode_for_column instead)" do
      assert {:error, :malformed} = Set.decode_tagged("bare")
      assert {:error, :malformed} = Set.decode_tagged(42)
    end

    test "rejects malformed envelopes" do
      assert {:error, :malformed} = Set.decode_tagged(["set"])
      assert {:error, :malformed} = Set.decode_tagged(["map", []])
    end
  end

  describe "decode_for_column/2 — schema-aware decode" do
    test "accepts bare value as 1-element set" do
      assert {:ok, %Set{elements: [42]}} =
               Set.decode_for_column(42, & &1)
    end

    test "accepts tagged form as multi-element set" do
      assert {:ok, %Set{elements: [1, 2, 3]}} =
               Set.decode_for_column(["set", [1, 2, 3]], & &1)
    end

    test "applies element_decoder to each element in tagged form" do
      assert {:ok, %Set{elements: [2, 4, 6]}} =
               Set.decode_for_column(["set", [1, 2, 3]], &(&1 * 2))
    end

    test "applies element_decoder to bare element" do
      assert {:ok, %Set{elements: [10]}} =
               Set.decode_for_column(5, &(&1 * 2))
    end
  end
end
