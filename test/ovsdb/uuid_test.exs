defmodule OVSDB.UUIDTest do
  use ExUnit.Case, async: true
  doctest OVSDB.UUID

  alias OVSDB.UUID

  describe "new/1" do
    test "creates a UUID from a valid canonical string" do
      uuid = UUID.new("550e8400-e29b-41d4-a716-446655440000")
      assert uuid.value == "550e8400-e29b-41d4-a716-446655440000"
    end

    test "raises on invalid strings" do
      for bad <- [
            "not-a-uuid",
            "550e8400-e29b-41d4-a716-44665544000",
            "550e8400-e29b-41d4-a716-4466554400000",
            "",
            "550e8400e29b41d4a716446655440000"
          ] do
        assert_raise ArgumentError, fn -> UUID.new(bad) end
      end
    end
  end

  describe "parse/1" do
    test "returns {:ok, uuid} for valid strings" do
      assert {:ok, %UUID{value: "550e8400-e29b-41d4-a716-446655440000"}} =
               UUID.parse("550e8400-e29b-41d4-a716-446655440000")
    end

    test "normalizes uppercase to lowercase" do
      assert {:ok, %UUID{value: "550e8400-e29b-41d4-a716-446655440000"}} =
               UUID.parse("550E8400-E29B-41D4-A716-446655440000")
    end

    test "returns {:error, :invalid_uuid} on malformed strings" do
      assert {:error, :invalid_uuid} = UUID.parse("not a uuid")
      assert {:error, :invalid_uuid} = UUID.parse("")
    end

    test "rejects non-binary input" do
      assert {:error, :invalid_uuid} = UUID.parse(123)
      assert {:error, :invalid_uuid} = UUID.parse(nil)
    end
  end

  describe "generate/0" do
    test "produces a valid v4 UUID" do
      uuid = UUID.generate()
      assert byte_size(uuid.value) == 36
      assert {:ok, ^uuid} = UUID.parse(uuid.value)
    end

    test "sets version bits to 4 and variant bits to 10xx" do
      for _ <- 1..100 do
        uuid = UUID.generate()

        <<_::binary-size(14), version::binary-size(1), _::binary-size(4), variant::binary-size(1),
          _::binary>> = uuid.value

        assert version == "4", "expected version 4, got #{version} in #{uuid.value}"

        assert variant in ~w(8 9 a b),
               "expected variant 8/9/a/b, got #{variant} in #{uuid.value}"
      end
    end

    test "produces uniformly-distributed variant bits" do
      variants =
        for _ <- 1..1000 do
          uuid = UUID.generate()
          <<_::binary-size(19), variant::binary-size(1), _::binary>> = uuid.value
          variant
        end

      counts = Enum.frequencies(variants)
      # Each variant should appear ~250 times ± a few stddev
      for v <- ~w(8 9 a b) do
        assert counts[v] > 150, "variant #{v} appeared only #{counts[v]} times in 1000"
        assert counts[v] < 350, "variant #{v} appeared #{counts[v]} times in 1000"
      end
    end

    test "produces unique UUIDs" do
      uuids = for _ <- 1..1000, do: UUID.generate().value
      assert length(uuids) == length(Enum.uniq(uuids))
    end
  end

  describe "encode/1 and decode/1" do
    test "round-trip preserves value" do
      uuid = UUID.new("550e8400-e29b-41d4-a716-446655440000")
      assert {:ok, ^uuid} = UUID.decode(UUID.encode(uuid))
    end

    test "encode produces RFC 7047 wire form" do
      uuid = UUID.new("550e8400-e29b-41d4-a716-446655440000")
      assert UUID.encode(uuid) == ["uuid", "550e8400-e29b-41d4-a716-446655440000"]
    end

    test "decode rejects non-uuid tagged forms" do
      assert {:error, :malformed} = UUID.decode(["set", []])
      assert {:error, :malformed} = UUID.decode(["named-uuid", "foo"])
    end

    test "decode rejects invalid UUID strings inside the wire form" do
      assert {:error, :invalid_uuid} = UUID.decode(["uuid", "not-a-uuid"])
    end

    test "decode rejects non-list input" do
      assert {:error, :malformed} = UUID.decode("bare string")
      assert {:error, :malformed} = UUID.decode(%{})
      assert {:error, :malformed} = UUID.decode(nil)
    end
  end
end
