defmodule OVSDB.RowTest do
  use ExUnit.Case, async: true

  alias OVSDB.{Row, UUID}

  describe "new/1" do
    test "creates a row with no columns when called without args" do
      assert Row.new() == %Row{uuid: nil, version: nil, columns: %{}}
    end

    test "wraps a column map" do
      columns = %{"name" => "br-lan", "ofport" => 42}
      assert Row.new(columns) == %Row{uuid: nil, version: nil, columns: columns}
    end
  end

  describe "get/3" do
    test "returns the column value when present" do
      row = Row.new(%{"name" => "br-lan"})
      assert Row.get(row, "name") == "br-lan"
    end

    test "returns nil for absent column by default" do
      row = Row.new(%{"name" => "br-lan"})
      assert Row.get(row, "missing") == nil
    end

    test "returns custom default for absent column" do
      row = Row.new(%{})
      assert Row.get(row, "missing", :not_found) == :not_found
    end

    test "returns uuid from struct field when asked for \"_uuid\"" do
      uuid = UUID.generate()
      row = %Row{uuid: uuid, columns: %{}}
      assert Row.get(row, "_uuid") == uuid
    end

    test "returns version from struct field when asked for \"_version\"" do
      version = UUID.generate()
      row = %Row{version: version, columns: %{}}
      assert Row.get(row, "_version") == version
    end

    test "returns default for \"_uuid\" when not set" do
      row = Row.new()
      assert Row.get(row, "_uuid", :absent) == :absent
    end
  end

  describe "put/3" do
    test "adds a regular column" do
      row = Row.new() |> Row.put("name", "br-lan")
      assert Row.get(row, "name") == "br-lan"
    end

    test "overwrites an existing column" do
      row =
        Row.new(%{"name" => "old"})
        |> Row.put("name", "new")

      assert Row.get(row, "name") == "new"
    end

    test "sets uuid via the \"_uuid\" shortcut" do
      uuid = UUID.generate()
      row = Row.new() |> Row.put("_uuid", uuid)
      assert row.uuid == uuid
      # Should not appear in columns map
      refute Elixir.Map.has_key?(row.columns, "_uuid")
    end

    test "sets version via the \"_version\" shortcut" do
      version = UUID.generate()
      row = Row.new() |> Row.put("_version", version)
      assert row.version == version
      refute Elixir.Map.has_key?(row.columns, "_version")
    end
  end

  describe "has?/2" do
    test "returns true for a present column" do
      row = Row.new(%{"name" => "br-lan"})
      assert Row.has?(row, "name")
    end

    test "returns false for absent column" do
      row = Row.new(%{"name" => "br-lan"})
      refute Row.has?(row, "missing")
    end

    test "returns true for \"_uuid\" when uuid is set" do
      row = %Row{uuid: UUID.generate(), columns: %{}}
      assert Row.has?(row, "_uuid")
    end

    test "returns false for \"_uuid\" when uuid is nil" do
      refute Row.has?(Row.new(), "_uuid")
    end
  end

  describe "diff/2" do
    test "returns columns that changed between two rows" do
      old = Row.new(%{"name" => "old", "count" => 1, "stable" => "x"})
      new = Row.new(%{"name" => "new", "count" => 1, "stable" => "x"})
      assert Row.diff(old, new) == %{"name" => "new"}
    end

    test "returns new columns not in old" do
      old = Row.new(%{"a" => 1})
      new = Row.new(%{"a" => 1, "b" => 2})
      assert Row.diff(old, new) == %{"b" => 2}
    end

    test "returns empty map for unchanged rows" do
      row = Row.new(%{"a" => 1, "b" => 2})
      assert Row.diff(row, row) == %{}
    end
  end

  describe "columns/1" do
    test "returns list of column names" do
      row = Row.new(%{"a" => 1, "b" => 2, "c" => 3})
      assert Enum.sort(Row.columns(row)) == ["a", "b", "c"]
    end

    test "returns empty list for row with no columns" do
      assert Row.columns(Row.new()) == []
    end

    test "does not include _uuid or _version even when set" do
      uuid = UUID.generate()
      row = %Row{uuid: uuid, columns: %{"name" => "x"}}
      assert Row.columns(row) == ["name"]
    end
  end
end
