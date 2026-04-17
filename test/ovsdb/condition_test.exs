defmodule OVSDB.ConditionTest do
  use ExUnit.Case, async: true

  alias OVSDB.{Condition, UUID}

  describe "operators/0" do
    test "returns all 8 RFC 7047 operators" do
      ops = Condition.operators()
      assert length(ops) == 8
      assert Enum.sort(ops) == [:eq, :excludes, :ge, :gt, :includes, :le, :lt, :ne]
    end
  end

  describe "operator_string/1" do
    test "returns the wire string for each operator" do
      assert Condition.operator_string(:eq) == "=="
      assert Condition.operator_string(:ne) == "!="
      assert Condition.operator_string(:lt) == "<"
      assert Condition.operator_string(:le) == "<="
      assert Condition.operator_string(:gt) == ">"
      assert Condition.operator_string(:ge) == ">="
      assert Condition.operator_string(:includes) == "includes"
      assert Condition.operator_string(:excludes) == "excludes"
    end

    test "raises on unknown operator" do
      assert_raise ArgumentError, ~r/unknown/, fn ->
        Condition.operator_string(:bogus)
      end
    end
  end

  describe "per-operator builders" do
    test "eq/2 builds a tuple" do
      assert Condition.eq("name", "br-lan") == {"name", :eq, "br-lan"}
    end

    test "ne/2" do
      assert Condition.ne("name", "br-lan") == {"name", :ne, "br-lan"}
    end

    test "lt/2" do
      assert Condition.lt("count", 10) == {"count", :lt, 10}
    end

    test "le/2" do
      assert Condition.le("count", 10) == {"count", :le, 10}
    end

    test "gt/2" do
      assert Condition.gt("count", 10) == {"count", :gt, 10}
    end

    test "ge/2" do
      assert Condition.ge("count", 10) == {"count", :ge, 10}
    end

    test "includes/2" do
      assert Condition.includes("ports", "x") == {"ports", :includes, "x"}
    end

    test "excludes/2" do
      assert Condition.excludes("ports", "x") == {"ports", :excludes, "x"}
    end
  end

  describe "new/3" do
    test "builds a condition tuple from column, op, value" do
      assert Condition.new("name", :eq, "br-lan") == {"name", :eq, "br-lan"}
    end

    test "validates the operator" do
      assert_raise ArgumentError, fn ->
        Condition.new("name", :bogus, "br-lan")
      end
    end
  end

  describe "encode/1" do
    test "produces [column, op_string, encoded_value] wire form" do
      assert Condition.encode({"name", :eq, "br-lan"}) == ["name", "==", "br-lan"]
    end

    test "encodes operator to wire string" do
      assert Condition.encode({"count", :ge, 5}) == ["count", ">=", 5]
      assert Condition.encode({"ports", :includes, "x"}) == ["ports", "includes", "x"]
    end

    test "walks value through Value.encode" do
      u = UUID.new("550e8400-e29b-41d4-a716-446655440000")

      assert Condition.encode({"_uuid", :eq, u}) == [
               "_uuid",
               "==",
               ["uuid", "550e8400-e29b-41d4-a716-446655440000"]
             ]
    end
  end

  describe "encode_all/1" do
    test "encodes a list of conditions" do
      conditions = [
        Condition.eq("name", "br-lan"),
        Condition.gt("count", 10)
      ]

      assert Condition.encode_all(conditions) == [
               ["name", "==", "br-lan"],
               ["count", ">", 10]
             ]
    end

    test "accepts empty list" do
      assert Condition.encode_all([]) == []
    end
  end
end
