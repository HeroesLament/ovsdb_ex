defmodule OVSDB.MapTest do
  use ExUnit.Case, async: true

  alias OVSDB.Map, as: OMap

  describe "new/1" do
    test "accepts a list of tuples" do
      assert OMap.new([{"k1", "v1"}, {"k2", "v2"}]) ==
               %OMap{entries: [{"k1", "v1"}, {"k2", "v2"}]}
    end

    test "accepts an Elixir map" do
      m = OMap.new(%{"k1" => "v1", "k2" => "v2"})
      # Order from Elixir map iteration isn't guaranteed, but size should be right
      assert OMap.size(m) == 2
      assert OMap.get(m, "k1") == "v1"
      assert OMap.get(m, "k2") == "v2"
    end

    test "preserves order when given a list" do
      # Tuple list preserves order; important for round-trips
      m = OMap.new([{"b", 1}, {"a", 2}, {"c", 3}])
      assert m.entries == [{"b", 1}, {"a", 2}, {"c", 3}]
    end

    test "handles empty input" do
      assert OMap.new([]) == %OMap{entries: []}
      assert OMap.new(%{}) == %OMap{entries: []}
    end
  end

  describe "empty/0" do
    test "returns a map with no entries" do
      assert OMap.empty() == %OMap{entries: []}
    end
  end

  describe "size/1" do
    test "returns entry count" do
      assert OMap.size(OMap.empty()) == 0
      assert OMap.size(OMap.new([{"a", 1}])) == 1
      assert OMap.size(OMap.new([{"a", 1}, {"b", 2}, {"c", 3}])) == 3
    end
  end

  describe "get/3" do
    test "returns the value for a present key" do
      m = OMap.new([{"k", "v"}])
      assert OMap.get(m, "k") == "v"
    end

    test "returns nil for absent key by default" do
      m = OMap.new([{"k", "v"}])
      assert OMap.get(m, "missing") == nil
    end

    test "returns custom default for absent key" do
      m = OMap.new([{"k", "v"}])
      assert OMap.get(m, "missing", :not_found) == :not_found
    end
  end

  describe "equal?/2" do
    test "treats maps with same entries in any order as equal" do
      a = OMap.new([{"a", 1}, {"b", 2}])
      b = OMap.new([{"b", 2}, {"a", 1}])
      assert OMap.equal?(a, b)
    end

    test "distinguishes maps with different entries" do
      a = OMap.new([{"a", 1}])
      b = OMap.new([{"a", 2}])
      refute OMap.equal?(a, b)
    end

    test "treats empty maps as equal" do
      assert OMap.equal?(OMap.empty(), OMap.empty())
    end
  end

  describe "encode/1" do
    test "produces [\"map\", [[k, v], ...]] wire form" do
      m = OMap.new([{"k1", "v1"}, {"k2", "v2"}])
      assert OMap.encode(m) == ["map", [["k1", "v1"], ["k2", "v2"]]]
    end

    test "encodes empty map as [\"map\", []]" do
      assert OMap.encode(OMap.empty()) == ["map", []]
    end

    test "preserves entry order in wire form" do
      m = OMap.new([{"b", 1}, {"a", 2}])
      assert OMap.encode(m) == ["map", [["b", 1], ["a", 2]]]
    end
  end

  describe "decode/1" do
    test "decodes wire form" do
      wire = ["map", [["k1", "v1"], ["k2", "v2"]]]
      assert {:ok, %OMap{entries: [{"k1", "v1"}, {"k2", "v2"}]}} = OMap.decode(wire)
    end

    test "decodes empty map" do
      assert {:ok, %OMap{entries: []}} = OMap.decode(["map", []])
    end

    test "rejects malformed envelopes" do
      assert {:error, :malformed} = OMap.decode(["set", []])
      assert {:error, :malformed} = OMap.decode("bare")
      assert {:error, :malformed} = OMap.decode(["map"])
    end

    test "rejects malformed entries" do
      assert {:error, :malformed} = OMap.decode(["map", [["k"]]])
      assert {:error, :malformed} = OMap.decode(["map", [["k", "v", "extra"]]])
    end

    test "round-trips via encode" do
      original = OMap.new([{"a", 1}, {"b", 2}])
      wire = OMap.encode(original)
      assert {:ok, ^original} = OMap.decode(wire)
    end
  end

  describe "decode_with/3" do
    test "applies decoders to keys and values" do
      wire = ["map", [["1", "a"], ["2", "b"]]]
      key_decode = &String.to_integer/1
      value_decode = &String.upcase/1

      assert {:ok, m} = OMap.decode_with(wire, key_decode, value_decode)
      assert m.entries == [{1, "A"}, {2, "B"}]
    end
  end

  describe "to_elixir_map/1" do
    test "converts to native Elixir map" do
      m = OMap.new([{"a", 1}, {"b", 2}])
      assert OMap.to_elixir_map(m) == %{"a" => 1, "b" => 2}
    end

    test "duplicate keys use last-write-wins" do
      # Tuple list allows duplicates; native map collapses them
      m = %OMap{entries: [{"k", 1}, {"k", 2}]}
      assert OMap.to_elixir_map(m) == %{"k" => 2}
    end
  end
end
