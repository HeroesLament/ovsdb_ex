defmodule OVSDB.MapTest do
  use ExUnit.Case, async: true
  doctest OVSDB.Map

  alias OVSDB.{Map, UUID}

  describe "new/1 and empty/0" do
    test "creates a map from a list of 2-tuples" do
      assert %Map{entries: [{"a", 1}, {"b", 2}]} = Map.new([{"a", 1}, {"b", 2}])
    end

    test "creates a map from a native Elixir map" do
      m = Map.new(%{"a" => 1, "b" => 2})
      assert Map.equal?(m, Map.new([{"a", 1}, {"b", 2}]))
    end

    test "accepts an empty list" do
      assert %Map{entries: []} = Map.new([])
    end

    test "empty/0 returns the empty map" do
      assert %Map{entries: []} = Map.empty()
    end

    test "raises on non-tuple entries" do
      assert_raise ArgumentError, ~r/tuples/, fn ->
        Map.new([{"a", 1}, :not_a_tuple])
      end
    end
  end

  describe "size/1" do
    test "returns the number of entries" do
      assert Map.size(Map.empty()) == 0
      assert Map.size(Map.new([{"a", 1}])) == 1
      assert Map.size(Map.new([{"a", 1}, {"b", 2}, {"c", 3}])) == 3
    end
  end

  describe "get/3" do
    test "returns the value for an existing key" do
      m = Map.new([{"a", 1}, {"b", 2}])
      assert Map.get(m, "a") == 1
      assert Map.get(m, "b") == 2
    end

    test "returns nil for missing key by default" do
      assert Map.get(Map.empty(), "missing") == nil
    end

    test "returns the custom default for missing key" do
      assert Map.get(Map.empty(), "missing", :not_there) == :not_there
    end
  end

  describe "equal?/2" do
    test "returns true for same entries regardless of order" do
      assert Map.equal?(
               Map.new([{"a", 1}, {"b", 2}]),
               Map.new([{"b", 2}, {"a", 1}])
             )
    end

    test "returns true for both empty" do
      assert Map.equal?(Map.empty(), Map.empty())
    end

    test "returns false for different entries" do
      refute Map.equal?(
               Map.new([{"a", 1}]),
               Map.new([{"a", 1}, {"b", 2}])
             )
    end
  end

  describe "to_elixir_map/1" do
    test "converts to a native Elixir map" do
      m = Map.new([{"a", 1}, {"b", 2}])
      assert Map.to_elixir_map(m) == %{"a" => 1, "b" => 2}
    end

    test "empty map converts to %{}" do
      assert Map.to_elixir_map(Map.empty()) == %{}
    end
  end

  describe "encode/1 — RFC 7047 wire form" do
    test "empty map encodes as tagged empty array" do
      assert Map.encode(Map.empty()) == ["map", []]
    end

    test "single-entry map is still fully tagged (no bare optimization)" do
      assert Map.encode(Map.new([{"k", "v"}])) == ["map", [["k", "v"]]]
    end

    test "multi-entry map encodes entries as [k, v] pairs" do
      assert Map.encode(Map.new([{"k1", "v1"}, {"k2", "v2"}])) ==
               ["map", [["k1", "v1"], ["k2", "v2"]]]
    end
  end

  describe "decode/1" do
    test "decodes tagged empty map" do
      assert {:ok, %Map{entries: []}} = Map.decode(["map", []])
    end

    test "decodes single-entry map" do
      assert {:ok, %Map{entries: [{"k", "v"}]}} = Map.decode(["map", [["k", "v"]]])
    end

    test "decodes multi-entry map" do
      assert {:ok, %Map{entries: [{"a", 1}, {"b", 2}]}} =
               Map.decode(["map", [["a", 1], ["b", 2]]])
    end

    test "rejects wrong tag" do
      assert {:error, :malformed} = Map.decode(["set", []])
      assert {:error, :malformed} = Map.decode(["not-map", []])
    end

    test "rejects malformed entries" do
      assert {:error, :malformed} = Map.decode(["map", [["k"]]])
      assert {:error, :malformed} = Map.decode(["map", [["k", "v", "extra"]]])
      assert {:error, :malformed} = Map.decode(["map", ["not a pair"]])
    end

    test "rejects non-array input" do
      assert {:error, :malformed} = Map.decode("bare")
      assert {:error, :malformed} = Map.decode(%{})
      assert {:error, :malformed} = Map.decode(nil)
    end
  end

  describe "decode_with/3" do
    test "applies key_decoder and value_decoder" do
      uuid_wire = ["uuid", "550e8400-e29b-41d4-a716-446655440000"]
      val_decoder = fn w -> {:ok, u} = UUID.decode(w); u end

      {:ok, m} = Map.decode_with(["map", [["k", uuid_wire]]], &Function.identity/1, val_decoder)
      assert [{"k", %UUID{}}] = m.entries
    end

    test "rejects malformed entries" do
      assert {:error, :malformed} =
               Map.decode_with(
                 ["map", [["k"]]],
                 &Function.identity/1,
                 &Function.identity/1
               )
    end
  end

  describe "encode/decode round-trips" do
    test "all cardinalities round-trip" do
      for entries <- [
            [],
            [{"k", "v"}],
            [{"a", 1}, {"b", 2}, {"c", 3}],
            [{1, "int_key"}, {"str_key", 42}]
          ] do
        m = Map.new(entries)
        {:ok, recovered} = Map.decode(Map.encode(m))
        assert Map.equal?(m, recovered)
      end
    end
  end
end
