defmodule OVSDB.SchemaTest do
  use ExUnit.Case, async: true

  alias OVSDB.Schema

  @fixture Path.join([__DIR__, "..", "fixtures", "test.ovsschema"])

  setup_all do
    schema_json = @fixture |> File.read!() |> Jason.decode!()
    {:ok, schema} = Schema.parse(schema_json)
    %{schema: schema, schema_json: schema_json}
  end

  describe "parse/1" do
    test "accepts a well-formed schema", %{schema: schema} do
      assert schema.name == "Test"
      assert schema.version == "1.0.0"
      assert schema.cksum == "123456789 1234"
    end

    test "extracts all tables", %{schema: schema} do
      assert Enum.sort(Schema.table_names(schema)) == ["Bridge", "Port"]
    end

    test "rejects non-map input" do
      assert {:error, _} = Schema.parse("not a map")
      assert {:error, _} = Schema.parse(42)
    end

    test "rejects missing name or tables" do
      assert {:error, _} = Schema.parse(%{})
      assert {:error, _} = Schema.parse(%{"name" => "x"})
      assert {:error, _} = Schema.parse(%{"tables" => %{}})
    end
  end

  describe "parse_string/1" do
    test "accepts a JSON string" do
      json = File.read!(@fixture)
      assert {:ok, %Schema{name: "Test"}} = Schema.parse_string(json)
    end

    test "rejects malformed JSON" do
      assert {:error, _} = Schema.parse_string("{bad")
    end
  end

  describe "table/2" do
    test "returns {:ok, table} for known tables", %{schema: schema} do
      assert {:ok, %Schema.Table{name: "Bridge"}} = Schema.table(schema, "Bridge")
    end

    test "returns :error for unknown tables", %{schema: schema} do
      assert :error = Schema.table(schema, "Nonexistent")
    end
  end

  describe "column/3" do
    test "fetches a column from a table", %{schema: schema} do
      assert {:ok, %Schema.Column{name: "name", kind: :atomic, key_type: :string}} =
               Schema.column(schema, "Bridge", "name")
    end

    test "returns :error for unknown column", %{schema: schema} do
      assert :error = Schema.column(schema, "Bridge", "not_a_column")
    end

    test "returns :error for unknown table", %{schema: schema} do
      assert :error = Schema.column(schema, "Nonexistent", "name")
    end
  end

  describe "column kinds" do
    test "string atomic column", %{schema: schema} do
      {:ok, col} = Schema.column(schema, "Bridge", "name")
      assert col.kind == :atomic
      assert col.key_type == :string
      assert col.min == 1
      assert col.max == 1
    end

    test "optional (min=0, max=1) set collapses in kind", %{schema: schema} do
      # "datapath_id" declared as min=0, max=1 — optional string
      {:ok, col} = Schema.column(schema, "Bridge", "datapath_id")
      assert col.min == 0
      assert col.max == 1
    end

    test "uuid-typed set column references another table", %{schema: schema} do
      {:ok, col} = Schema.column(schema, "Bridge", "ports")
      assert col.kind == :set
      # key_type should carry the ref constraint
      case col.key_type do
        :uuid -> :ok
        {:ref, "Port", _strength} -> :ok
        other -> flunk("unexpected key_type for ref column: #{inspect(other)}")
      end
    end

    test "map column has both key_type and value_type", %{schema: schema} do
      {:ok, col} = Schema.column(schema, "Bridge", "external_ids")
      assert col.kind == :map
      assert col.key_type == :string
      assert col.value_type == :string
    end

    test "enum constraint is captured", %{schema: schema} do
      {:ok, col} = Schema.column(schema, "Bridge", "fail_mode")

      case col.key_type do
        {:enum, :string, values} ->
          assert Enum.sort(values) == ["secure", "standalone"]

        other ->
          flunk("expected enum constraint, got #{inspect(other)}")
      end
    end

    test "ranged integer constraint", %{schema: schema} do
      {:ok, col} = Schema.column(schema, "Bridge", "priority")
      assert match?({:ranged, :integer, 0, 65_535}, col.key_type)
    end

    test "bounded string constraint", %{schema: schema} do
      {:ok, col} = Schema.column(schema, "Port", "name")
      assert match?({:bounded_string, 1, 64}, col.key_type)
    end
  end

  describe "Column predicates" do
    test "atomic?/1, set?/1, map?/1", %{schema: schema} do
      {:ok, name_col} = Schema.column(schema, "Bridge", "name")
      {:ok, ports_col} = Schema.column(schema, "Bridge", "ports")
      {:ok, ext_col} = Schema.column(schema, "Bridge", "external_ids")

      assert Schema.Column.atomic?(name_col)
      refute Schema.Column.set?(name_col)
      refute Schema.Column.map?(name_col)

      assert Schema.Column.set?(ports_col)
      refute Schema.Column.atomic?(ports_col)

      assert Schema.Column.map?(ext_col)
    end

    test "optional?/1", %{schema: schema} do
      {:ok, name_col} = Schema.column(schema, "Bridge", "name")
      {:ok, dpid_col} = Schema.column(schema, "Bridge", "datapath_id")

      refute Schema.Column.optional?(name_col)
      assert Schema.Column.optional?(dpid_col)
    end
  end

  describe "validate_row/3 — happy path" do
    test "accepts a row with valid column values", %{schema: schema} do
      row = %{"name" => "br-lan"}
      assert :ok = Schema.validate_row(schema, "Bridge", row)
    end

    test "accepts empty row", %{schema: schema} do
      assert :ok = Schema.validate_row(schema, "Bridge", %{})
    end
  end

  describe "validate_row/3 — error paths" do
    test "rejects unknown table", %{schema: schema} do
      assert {:error, {:unknown_table, "Nonexistent"}} =
               Schema.validate_row(schema, "Nonexistent", %{})
    end

    test "rejects unknown column", %{schema: schema} do
      row = %{"not_a_column" => "value"}

      assert {:error, {:unknown_column, "Bridge", "not_a_column"}} =
               Schema.validate_row(schema, "Bridge", row)
    end

    test "rejects bad value type (string expected, got integer)", %{schema: schema} do
      row = %{"name" => 42}
      assert {:error, {:bad_value, "Bridge", "name", _}} = Schema.validate_row(schema, "Bridge", row)
    end
  end
end
