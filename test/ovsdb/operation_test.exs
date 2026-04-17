defmodule OVSDB.OperationTest do
  use ExUnit.Case, async: true

  alias OVSDB.{Condition, NamedUUID, Operation, Row, UUID}

  describe "insert/3" do
    test "builds an insert op from a map" do
      op = Operation.insert("Bridge", %{"name" => "br-lan"})

      assert op == %{
               "op" => "insert",
               "table" => "Bridge",
               "row" => %{"name" => "br-lan"}
             }
    end

    test "accepts a Row struct" do
      row = Row.new(%{"name" => "br-lan", "ofport" => 42})
      op = Operation.insert("Bridge", row)
      assert op["op"] == "insert"
      assert op["row"] == %{"name" => "br-lan", "ofport" => 42}
    end

    test "walks values through Value.encode" do
      u = UUID.new("550e8400-e29b-41d4-a716-446655440000")
      op = Operation.insert("Bridge", %{"parent" => u})
      assert op["row"] == %{"parent" => ["uuid", "550e8400-e29b-41d4-a716-446655440000"]}
    end

    test "adds uuid-name when :uuid_name option given" do
      op = Operation.insert("Bridge", %{"name" => "br"}, uuid_name: "new_br")
      assert op["uuid-name"] == "new_br"
    end

    test "does not include uuid-name key when option absent" do
      op = Operation.insert("Bridge", %{"name" => "br"})
      refute Elixir.Map.has_key?(op, "uuid-name")
    end
  end

  describe "select/3" do
    test "builds a select op with where clause" do
      where = [Condition.eq("name", "br-lan")]
      op = Operation.select("Bridge", where)

      assert op == %{
               "op" => "select",
               "table" => "Bridge",
               "where" => [["name", "==", "br-lan"]]
             }
    end

    test "includes column projection when specified" do
      where = [Condition.eq("name", "br-lan")]
      op = Operation.select("Bridge", where, ["_uuid", "ports"])
      assert op["columns"] == ["_uuid", "ports"]
    end

    test "omits columns key when nil" do
      op = Operation.select("Bridge", [])
      refute Elixir.Map.has_key?(op, "columns")
    end

    test "accepts empty where clause (select all)" do
      op = Operation.select("Bridge", [])
      assert op["where"] == []
    end
  end

  describe "update/3" do
    test "builds an update op" do
      where = [Condition.eq("_uuid", UUID.new("550e8400-e29b-41d4-a716-446655440000"))]
      op = Operation.update("Bridge", where, %{"name" => "new-name"})

      assert op == %{
               "op" => "update",
               "table" => "Bridge",
               "where" => [["_uuid", "==", ["uuid", "550e8400-e29b-41d4-a716-446655440000"]]],
               "row" => %{"name" => "new-name"}
             }
    end
  end

  describe "mutators/0 and mutator_string/1" do
    test "returns all RFC 7047 mutators" do
      muts = Operation.mutators() |> Enum.sort()
      assert muts == [:add, :delete, :div, :insert, :mod, :mul, :sub]
    end

    test "mutator_string maps atoms to wire strings" do
      assert Operation.mutator_string(:add) == "+="
      assert Operation.mutator_string(:sub) == "-="
      assert Operation.mutator_string(:mul) == "*="
      assert Operation.mutator_string(:div) == "/="
      assert Operation.mutator_string(:mod) == "%="
      assert Operation.mutator_string(:insert) == "insert"
      assert Operation.mutator_string(:delete) == "delete"
    end

    test "raises on unknown mutator" do
      assert_raise ArgumentError, fn -> Operation.mutator_string(:bogus) end
    end
  end

  describe "mutate/3" do
    test "builds a mutate op with a list of mutations" do
      where = [Condition.eq("name", "br")]
      op = Operation.mutate("Bridge", where, [{"count", :add, 1}])

      assert op == %{
               "op" => "mutate",
               "table" => "Bridge",
               "where" => [["name", "==", "br"]],
               "mutations" => [["count", "+=", 1]]
             }
    end

    test "encodes multiple mutations in order" do
      op =
        Operation.mutate("T", [], [
          {"a", :add, 1},
          {"b", :sub, 2}
        ])

      assert op["mutations"] == [["a", "+=", 1], ["b", "-=", 2]]
    end

    test "walks mutation values through Value.encode" do
      u = UUID.new("550e8400-e29b-41d4-a716-446655440000")
      op = Operation.mutate("T", [], [{"ref", :insert, u}])

      assert op["mutations"] == [
               ["ref", "insert", ["uuid", "550e8400-e29b-41d4-a716-446655440000"]]
             ]
    end
  end

  describe "delete/2" do
    test "builds a delete op" do
      op = Operation.delete("Bridge", [Condition.eq("name", "br-lan")])

      assert op == %{
               "op" => "delete",
               "table" => "Bridge",
               "where" => [["name", "==", "br-lan"]]
             }
    end

    test "accepts empty where (delete all)" do
      op = Operation.delete("Bridge", [])
      assert op["where"] == []
    end
  end

  describe "wait/5" do
    test "builds a wait op with :until => :eq" do
      op = Operation.wait("Bridge", [], ["name"], [%{"name" => "br"}], until: :eq, timeout: 0)

      assert op["op"] == "wait"
      assert op["table"] == "Bridge"
      assert op["where"] == []
      assert op["columns"] == ["name"]
      assert op["rows"] == [%{"name" => "br"}]
      assert op["until"] == "=="
      assert op["timeout"] == 0
    end

    test "supports :until => :ne" do
      op = Operation.wait("T", [], ["x"], [%{"x" => 1}], until: :ne, timeout: 100)
      assert op["until"] == "!="
      assert op["timeout"] == 100
    end

    test "raises on invalid timeout" do
      assert_raise ArgumentError, fn ->
        Operation.wait("T", [], ["x"], [%{}], until: :eq, timeout: -1)
      end
    end
  end

  describe "commit/1" do
    test "without durable flag" do
      assert Operation.commit() == %{"op" => "commit", "durable" => false}
    end

    test "with durable: true" do
      assert Operation.commit(true) == %{"op" => "commit", "durable" => true}
    end
  end

  describe "abort/0" do
    test "builds an abort op" do
      assert Operation.abort() == %{"op" => "abort"}
    end
  end

  describe "comment/1" do
    test "builds a comment op with text" do
      op = Operation.comment("bridge reconfiguration")

      assert op == %{
               "op" => "comment",
               "comment" => "bridge reconfiguration"
             }
    end
  end

  describe "assert_lock/1" do
    test "builds an assert op with lock name" do
      op = Operation.assert_lock("my_lock")

      assert op == %{
               "op" => "assert",
               "lock" => "my_lock"
             }
    end
  end

  describe "integration — insert with NamedUUID reference" do
    test "combines insert with a named-uuid value" do
      named = NamedUUID.new("new_br")
      op = Operation.insert("Port", %{"bridge" => named})

      assert op["row"] == %{"bridge" => ["named-uuid", "new_br"]}
    end
  end
end
