defmodule OVSDB.RowTest do
  use ExUnit.Case, async: true
  doctest OVSDB.Row

  alias OVSDB.{Row, UUID}

  setup do
    uuid = UUID.new("550e8400-e29b-41d4-a716-446655440000")
    version = UUID.new("11111111-2222-4333-8444-555555555555")
    {:ok, uuid: uuid, version: version}
  end

  describe "new/1" do
    test "creates a row from a column map" do
      row = Row.new(%{"name" => "br-lan", "ofport" => 42})
      assert row.columns == %{"name" => "br-lan", "ofport" => 42}
      assert row.uuid == nil
      assert row.version == nil
    end

    test "creates an empty row with no args" do
      row = Row.new()
      assert row.columns == %{}
      assert row.uuid == nil
      assert row.version == nil
    end
  end

  describe "get/3" do
    test "returns the value for a column" do
      row = Row.new(%{"name" => "br-lan"})
      assert Row.get(row, "name") == "br-lan"
    end

    test "returns nil for missing column by default" do
      assert Row.get(Row.new(), "missing") == nil
    end

    test "returns custom default for missing column" do
      assert Row.get(Row.new(), "missing", :absent) == :absent
    end

    test "returns the struct field for _uuid when set", %{uuid: uuid} do
      row = Row.put(Row.new(), "_uuid", uuid)
      assert Row.get(row, "_uuid") == uuid
    end

    test "returns the struct field for _version when set", %{version: version} do
      row = Row.put(Row.new(), "_version", version)
      assert Row.get(row, "_version") == version
    end

    test "returns default when _uuid is not set" do
      assert Row.get(Row.new(), "_uuid") == nil
      assert Row.get(Row.new(), "_uuid", :no_uuid) == :no_uuid
    end
  end

  describe "put/3" do
    test "adds a column value" do
      row = Row.new() |> Row.put("name", "br-lan")
      assert Row.get(row, "name") == "br-lan"
    end

    test "overwrites existing column" do
      row = Row.new(%{"name" => "old"}) |> Row.put("name", "new")
      assert Row.get(row, "name") == "new"
    end

    test "routes _uuid to struct field", %{uuid: uuid} do
      row = Row.put(Row.new(), "_uuid", uuid)
      assert row.uuid == uuid
      assert row.columns == %{}
    end

    test "routes _version to struct field", %{version: version} do
      row = Row.put(Row.new(), "_version", version)
      assert row.version == version
      assert row.columns == %{}
    end
  end

  describe "has?/2" do
    test "returns true when column exists" do
      assert Row.has?(Row.new(%{"name" => "x"}), "name")
    end

    test "returns false when column is absent" do
      refute Row.has?(Row.new(), "nope")
    end

    test "returns true for _uuid when set", %{uuid: uuid} do
      row = Row.put(Row.new(), "_uuid", uuid)
      assert Row.has?(row, "_uuid")
    end

    test "returns false for _uuid when not set" do
      refute Row.has?(Row.new(), "_uuid")
    end

    test "returns true for _version when set", %{version: version} do
      row = Row.put(Row.new(), "_version", version)
      assert Row.has?(row, "_version")
    end
  end

  describe "columns/1" do
    test "returns all column names" do
      row = Row.new(%{"a" => 1, "b" => 2, "c" => 3})
      assert Enum.sort(Row.columns(row)) == ["a", "b", "c"]
    end

    test "excludes _uuid metadata", %{uuid: uuid} do
      row = Row.put(Row.new(%{"name" => "br"}), "_uuid", uuid)
      assert Row.columns(row) == ["name"]
    end

    test "returns empty list for empty row" do
      assert Row.columns(Row.new()) == []
    end
  end

  describe "diff/2" do
    test "returns only the columns that differ" do
      old = Row.new(%{"name" => "br", "ofport" => 1, "keep" => true})
      new = Row.new(%{"name" => "br", "ofport" => 2, "keep" => true, "added" => "x"})
      assert Row.diff(old, new) == %{"ofport" => 2, "added" => "x"}
    end

    test "returns empty map when identical" do
      r = Row.new(%{"a" => 1, "b" => 2})
      assert Row.diff(r, r) == %{}
    end

    test "ignores _uuid metadata differences", %{uuid: uuid} do
      r1 = Row.put(Row.new(%{"col" => 1}), "_uuid", uuid)
      r2 = Row.new(%{"col" => 1})
      assert Row.diff(r1, r2) == %{}
      assert Row.diff(r2, r1) == %{}
    end
  end
end
