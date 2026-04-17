defmodule OVSDB.SchemaHelperTest do
  use ExUnit.Case, async: true

  alias OVSDB.{MonitorSpec, Schema, SchemaHelper}

  @fixture Path.join([__DIR__, "..", "fixtures", "test.ovsschema"])

  setup_all do
    schema_json = File.read!(@fixture) |> Jason.decode!()
    {:ok, schema} = Schema.parse(schema_json)
    %{schema: schema}
  end

  describe "new/1" do
    test "wraps a Schema with no registrations", %{schema: schema} do
      helper = SchemaHelper.new(schema)
      assert helper.source == schema
      assert helper.registrations == %{}
      assert SchemaHelper.empty?(helper)
    end
  end

  describe "register_table/2" do
    test "records a table as :all columns", %{schema: schema} do
      {:ok, helper} = SchemaHelper.new(schema) |> SchemaHelper.register_table("Bridge")
      assert helper.registrations == %{"Bridge" => :all}
    end

    test "rejects unknown tables", %{schema: schema} do
      assert {:error, _} =
               SchemaHelper.new(schema) |> SchemaHelper.register_table("Nonexistent")
    end
  end

  describe "register_table!/2" do
    test "returns updated helper on success", %{schema: schema} do
      helper = SchemaHelper.new(schema) |> SchemaHelper.register_table!("Bridge")
      assert helper.registrations["Bridge"] == :all
    end

    test "raises on unknown table", %{schema: schema} do
      assert_raise ArgumentError, fn ->
        SchemaHelper.new(schema) |> SchemaHelper.register_table!("Nonexistent")
      end
    end
  end

  describe "register_columns/3" do
    test "records specific columns", %{schema: schema} do
      {:ok, helper} =
        SchemaHelper.new(schema)
        |> SchemaHelper.register_columns("Bridge", ["name", "ports"])

      assert {:columns, mapset} = helper.registrations["Bridge"]
      assert MapSet.to_list(mapset) |> Enum.sort() == ["name", "ports"]
    end

    test "merges columns on repeated calls", %{schema: schema} do
      {:ok, helper} =
        SchemaHelper.new(schema)
        |> SchemaHelper.register_columns("Bridge", ["name"])

      {:ok, helper} = SchemaHelper.register_columns(helper, "Bridge", ["ports"])

      assert {:columns, mapset} = helper.registrations["Bridge"]
      assert MapSet.to_list(mapset) |> Enum.sort() == ["name", "ports"]
    end

    test "rejects unknown columns", %{schema: schema} do
      assert {:error, _} =
               SchemaHelper.new(schema)
               |> SchemaHelper.register_columns("Bridge", ["not_a_column"])
    end

    test "rejects unknown tables", %{schema: schema} do
      assert {:error, _} =
               SchemaHelper.new(schema)
               |> SchemaHelper.register_columns("Nonexistent", ["name"])
    end
  end

  describe "precedence — all beats some" do
    test "register_table after register_columns keeps :all", %{schema: schema} do
      helper =
        SchemaHelper.new(schema)
        |> SchemaHelper.register_columns!("Bridge", ["name"])
        |> SchemaHelper.register_table!("Bridge")

      assert helper.registrations["Bridge"] == :all
    end

    test "register_columns after register_table stays :all", %{schema: schema} do
      helper =
        SchemaHelper.new(schema)
        |> SchemaHelper.register_table!("Bridge")
        |> SchemaHelper.register_columns!("Bridge", ["name"])

      assert helper.registrations["Bridge"] == :all
    end
  end

  describe "introspection" do
    test "registered_tables/1", %{schema: schema} do
      helper =
        SchemaHelper.new(schema)
        |> SchemaHelper.register_table!("Bridge")
        |> SchemaHelper.register_columns!("Port", ["name"])

      assert Enum.sort(SchemaHelper.registered_tables(helper)) == ["Bridge", "Port"]
    end

    test "registered_columns/2 returns all columns when :all", %{schema: schema} do
      helper =
        SchemaHelper.new(schema)
        |> SchemaHelper.register_table!("Bridge")

      cols = SchemaHelper.registered_columns(helper, "Bridge")
      # Expect all schema columns
      assert "name" in cols
      assert "ports" in cols
    end

    test "registered_columns/2 returns specific columns", %{schema: schema} do
      helper =
        SchemaHelper.new(schema)
        |> SchemaHelper.register_columns!("Bridge", ["name"])

      assert SchemaHelper.registered_columns(helper, "Bridge") == ["name"]
    end
  end

  describe "get_idl_schema/1" do
    test "returns :error for empty helper", %{schema: schema} do
      assert {:error, :no_registrations} =
               SchemaHelper.new(schema) |> SchemaHelper.get_idl_schema()
    end

    test "returns filtered schema with only registered tables", %{schema: schema} do
      helper =
        SchemaHelper.new(schema)
        |> SchemaHelper.register_table!("Bridge")

      {:ok, filtered} = SchemaHelper.get_idl_schema(helper)
      assert Schema.table_names(filtered) == ["Bridge"]
      refute "Port" in Schema.table_names(filtered)
    end

    test "filtered schema preserves schema metadata (name, version)", %{schema: schema} do
      helper = SchemaHelper.new(schema) |> SchemaHelper.register_table!("Bridge")
      {:ok, filtered} = SchemaHelper.get_idl_schema(helper)
      assert filtered.name == schema.name
      assert filtered.version == schema.version
    end
  end

  describe "get_monitor_spec/2" do
    test "produces a MonitorSpec covering registered tables", %{schema: schema} do
      helper =
        SchemaHelper.new(schema)
        |> SchemaHelper.register_table!("Bridge")

      spec = SchemaHelper.get_monitor_spec(helper, "my-monitor")

      assert %MonitorSpec{} = spec
      assert spec.db == schema.name
      assert spec.monitor_id == "my-monitor"
      assert "Bridge" in MonitorSpec.tables(spec)
    end

    test "only specified columns when registered with register_columns", %{schema: schema} do
      helper =
        SchemaHelper.new(schema)
        |> SchemaHelper.register_columns!("Bridge", ["name"])

      spec = SchemaHelper.get_monitor_spec(helper, "mon1")
      assert spec.tables["Bridge"][:columns] == ["name"]
    end
  end
end
