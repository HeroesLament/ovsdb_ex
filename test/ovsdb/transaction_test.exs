defmodule OVSDB.TransactionTest do
  use ExUnit.Case, async: true

  alias OVSDB.{Operation, Transaction}

  describe "new/1" do
    test "creates an empty transaction for a database" do
      assert Transaction.new("Open_vSwitch") == %Transaction{db: "Open_vSwitch", ops: []}
    end
  end

  describe "add/2" do
    test "appends operations in order" do
      op1 = Operation.insert("Bridge", %{"name" => "br1"})
      op2 = Operation.insert("Bridge", %{"name" => "br2"})

      txn =
        Transaction.new("db")
        |> Transaction.add(op1)
        |> Transaction.add(op2)

      assert txn.ops == [op1, op2]
    end
  end

  describe "prepend/2" do
    test "prepends operations so the latest prepend runs first" do
      op1 = Operation.insert("Bridge", %{"name" => "br1"})
      op2 = Operation.insert("Bridge", %{"name" => "br2"})

      txn =
        Transaction.new("db")
        |> Transaction.prepend(op1)
        |> Transaction.prepend(op2)

      # Last prepended goes to the front
      assert txn.ops == [op2, op1]
    end
  end

  describe "size/1 and empty?/1" do
    test "empty transaction has size 0 and is empty?" do
      txn = Transaction.new("db")
      assert Transaction.size(txn) == 0
      assert Transaction.empty?(txn)
    end

    test "after add/2 size reflects op count" do
      txn =
        Transaction.new("db")
        |> Transaction.add(Operation.abort())
        |> Transaction.add(Operation.abort())

      assert Transaction.size(txn) == 2
      refute Transaction.empty?(txn)
    end
  end

  describe "to_params/1" do
    test "returns [db | ops] per RFC 7047 §4.1.3" do
      op = Operation.insert("Bridge", %{"name" => "br"})

      params =
        Transaction.new("Open_vSwitch")
        |> Transaction.add(op)
        |> Transaction.to_params()

      assert params == ["Open_vSwitch", op]
    end

    test "empty transaction returns just [db]" do
      assert Transaction.to_params(Transaction.new("db")) == ["db"]
    end
  end

  describe "to_request/2" do
    test "wraps to_params in a full transact request" do
      op = Operation.insert("T", %{"x" => 1})

      req =
        Transaction.new("MyDB")
        |> Transaction.add(op)
        |> Transaction.to_request(42)

      assert req["method"] == "transact"
      assert req["id"] == 42
      assert req["params"] == ["MyDB", op]
    end
  end
end
