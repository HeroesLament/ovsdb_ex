defmodule OVSDB.ProtocolTest do
  use ExUnit.Case, async: true

  alias OVSDB.Protocol

  describe "request/3" do
    test "builds a request map with id" do
      req = Protocol.request("list_dbs", [], 1)
      assert req == %{"method" => "list_dbs", "params" => [], "id" => 1}
    end

    test "accepts string ids too" do
      req = Protocol.request("echo", ["ping"], "req-42")
      assert req["id"] == "req-42"
    end

    test "preserves params as given" do
      req = Protocol.request("transact", ["Open_vSwitch", %{"op" => "select"}], 5)
      assert req["params"] == ["Open_vSwitch", %{"op" => "select"}]
    end
  end

  describe "response/2" do
    test "builds a success response" do
      resp = Protocol.response(1, ["Open_vSwitch"])
      assert resp == %{"id" => 1, "result" => ["Open_vSwitch"], "error" => nil}
    end

    test "result can be any term" do
      resp = Protocol.response(1, %{"count" => 1})
      assert resp["result"] == %{"count" => 1}
    end
  end

  describe "error_response/3" do
    test "builds an error response" do
      resp = Protocol.error_response(1, "not supported")
      assert resp == %{"id" => 1, "result" => nil, "error" => "not supported"}
    end

    test "with details, error field becomes an object with error+details" do
      # RFC 7047 §3.1: when details are provided, the "error" field
      # contains an object with both "error" and "details" members.
      resp = Protocol.error_response(1, "constraint violation", "column 'x' is required")

      assert resp["error"] == %{
               "error" => "constraint violation",
               "details" => "column 'x' is required"
             }

      assert resp["id"] == 1
      assert resp["result"] == nil
    end
  end

  describe "notification/2" do
    test "builds a notification with id=null" do
      note = Protocol.notification("update", ["monitor-1", %{}])
      assert note == %{"method" => "update", "params" => ["monitor-1", %{}], "id" => nil}
    end
  end

  describe "classify/1 — requests" do
    test "recognizes a request (method + params + integer id)" do
      msg = %{"method" => "list_dbs", "params" => [], "id" => 1}
      assert {:ok, {:request, %{id: 1, method: "list_dbs", params: []}}} =
               Protocol.classify(msg)
    end

    test "recognizes a request with string id" do
      msg = %{"method" => "echo", "params" => ["ping"], "id" => "req-42"}
      assert {:ok, {:request, %{id: "req-42"}}} = Protocol.classify(msg)
    end
  end

  describe "classify/1 — notifications" do
    test "recognizes a notification (method + params + id=null)" do
      msg = %{"method" => "update", "params" => ["m", %{}], "id" => nil}
      assert {:ok, {:notification, %{method: "update", params: ["m", %{}]}}} =
               Protocol.classify(msg)
    end
  end

  describe "classify/1 — responses" do
    test "recognizes a success response" do
      msg = %{"id" => 1, "result" => ["x"], "error" => nil}
      assert {:ok, {:response, %{id: 1, result: ["x"], error: nil}}} =
               Protocol.classify(msg)
    end

    test "recognizes an error response" do
      msg = %{"id" => 1, "result" => nil, "error" => "boom"}
      assert {:ok, {:response, %{id: 1, result: nil, error: "boom"}}} =
               Protocol.classify(msg)
    end
  end

  describe "classify/1 — errors" do
    test "rejects non-map input" do
      assert {:error, {:not_a_map, "string"}} = Protocol.classify("string")
      assert {:error, {:not_a_map, 42}} = Protocol.classify(42)
    end

    test "rejects method-bearing message without params" do
      msg = %{"method" => "list_dbs", "id" => 1}
      assert {:error, {:missing_fields, missing}} = Protocol.classify(msg)
      assert "params" in missing
    end

    test "rejects method-bearing message without id" do
      msg = %{"method" => "list_dbs", "params" => []}
      assert {:error, {:missing_fields, missing}} = Protocol.classify(msg)
      assert "id" in missing
    end

    test "rejects completely unclassifiable messages" do
      assert {:error, :unclassifiable} = Protocol.classify(%{"random" => "thing"})
    end
  end

  describe "serialize/1" do
    test "produces iodata that round-trips through parse" do
      req = Protocol.request("list_dbs", [], 1)
      iodata = Protocol.serialize(req)
      binary = IO.iodata_to_binary(iodata)
      assert {:ok, parsed} = Protocol.parse(binary)
      assert parsed == req
    end

    test "serializes nested data correctly" do
      msg =
        Protocol.request("transact", ["db", %{"op" => "insert", "row" => %{"x" => 1}}], 2)

      binary = Protocol.serialize(msg) |> IO.iodata_to_binary()
      assert {:ok, parsed} = Protocol.parse(binary)
      assert parsed == msg
    end
  end

  describe "parse/1" do
    test "parses valid JSON object" do
      assert {:ok, %{"id" => 1}} = Protocol.parse(~s({"id":1}))
    end

    test "rejects invalid JSON" do
      assert {:error, _} = Protocol.parse(~s({"id":}))
      assert {:error, _} = Protocol.parse("not json")
    end

    test "rejects non-object JSON (arrays, primitives)" do
      assert {:error, _} = Protocol.parse("[1,2,3]")
      assert {:error, _} = Protocol.parse("42")
      assert {:error, _} = Protocol.parse(~s("just a string"))
    end
  end

  describe "known_methods/0 and known_method?/1" do
    test "returns a non-empty list of method strings" do
      methods = Protocol.known_methods()
      assert is_list(methods)
      assert methods != []
      assert Enum.all?(methods, &is_binary/1)
    end

    test "includes expected core methods" do
      methods = Protocol.known_methods()
      assert "list_dbs" in methods
      assert "get_schema" in methods
      assert "transact" in methods
      assert "monitor" in methods
      assert "echo" in methods
    end

    test "known_method? accepts known method names" do
      assert Protocol.known_method?("list_dbs")
      assert Protocol.known_method?("transact")
    end

    test "known_method? rejects unknown or non-binary input" do
      refute Protocol.known_method?("not_a_method")
      refute Protocol.known_method?(:atom)
      refute Protocol.known_method?(42)
    end
  end
end
