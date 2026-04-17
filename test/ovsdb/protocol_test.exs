defmodule OVSDB.ProtocolTest do
  use ExUnit.Case, async: true
  doctest OVSDB.Protocol

  alias OVSDB.Protocol

  describe "known_method?/1 and known_methods/0" do
    test "recognizes all client-side methods" do
      for m <- ~w(list_dbs get_schema transact cancel monitor monitor_cancel
                  lock steal unlock echo) do
        assert Protocol.known_method?(m), "expected #{m} to be known"
      end
    end

    test "recognizes all server-side notifications" do
      for m <- ~w(update locked stolen echo) do
        assert Protocol.known_method?(m), "expected #{m} to be known"
      end
    end

    test "rejects unknown methods" do
      refute Protocol.known_method?("nonsense")
      refute Protocol.known_method?("")
    end

    test "rejects non-string inputs" do
      refute Protocol.known_method?(42)
      refute Protocol.known_method?(nil)
      refute Protocol.known_method?(:transact)
    end

    test "known_methods/0 returns all methods deduplicated" do
      methods = Protocol.known_methods()
      assert length(methods) == length(Enum.uniq(methods))
      assert "transact" in methods
      assert "update" in methods
      assert "echo" in methods
    end
  end

  describe "request/3" do
    test "builds a request map" do
      assert %{"method" => "list_dbs", "params" => [], "id" => 1} =
               Protocol.request("list_dbs", [], 1)
    end

    test "accepts string ids" do
      assert %{"id" => "my-id"} = Protocol.request("get_schema", ["db"], "my-id")
    end

    test "accepts complex params" do
      params = ["db", %{"op" => "select", "table" => "Bridge"}]
      assert %{"params" => ^params} = Protocol.request("transact", params, 1)
    end

    test "accepts any method name (vendor extensions allowed)" do
      assert %{"method" => "vendor_extension"} = Protocol.request("vendor_extension", [], 1)
    end
  end

  describe "response/2" do
    test "builds a successful response with null error" do
      assert %{"result" => ["Open_vSwitch"], "error" => nil, "id" => 1} =
               Protocol.response(1, ["Open_vSwitch"])
    end

    test "accepts any result shape" do
      assert %{"result" => []} = Protocol.response(1, [])
      assert %{"result" => %{"nested" => "map"}} = Protocol.response(1, %{"nested" => "map"})
    end
  end

  describe "error_response/3" do
    test "builds a simple error response" do
      assert %{"result" => nil, "error" => "unknown database", "id" => 1} =
               Protocol.error_response(1, "unknown database")
    end

    test "nests error + details when details provided" do
      assert %{
               "error" => %{
                 "error" => "syntax error",
                 "details" => "unexpected token"
               }
             } = Protocol.error_response(1, "syntax error", "unexpected token")
    end

    test "bare error string when details is nil" do
      assert %{"error" => "bare"} = Protocol.error_response(1, "bare", nil)
    end
  end

  describe "notification/2" do
    test "builds a notification with null id" do
      assert %{"method" => "echo", "params" => [], "id" => nil} =
               Protocol.notification("echo", [])
    end

    test "distinguishes from a request by null id" do
      req = Protocol.request("update", [], 1)
      notif = Protocol.notification("update", [])

      assert req["id"] == 1
      assert notif["id"] == nil
    end
  end

  describe "classify/1 — happy paths" do
    test "classifies a request" do
      msg = %{"method" => "list_dbs", "params" => [], "id" => 1}
      assert {:ok, {:request, %{id: 1, method: "list_dbs", params: []}}} = Protocol.classify(msg)
    end

    test "classifies a notification (method with null id)" do
      msg = %{"method" => "update", "params" => ["m", %{}], "id" => nil}

      assert {:ok, {:notification, %{method: "update", params: ["m", %{}]}}} =
               Protocol.classify(msg)
    end

    test "classifies a success response" do
      msg = %{"result" => [1, 2], "error" => nil, "id" => 1}
      assert {:ok, {:response, %{id: 1, result: [1, 2], error: nil}}} = Protocol.classify(msg)
    end

    test "classifies an error response" do
      msg = %{"result" => nil, "error" => "unknown database", "id" => 1}

      assert {:ok, {:response, %{id: 1, result: nil, error: "unknown database"}}} =
               Protocol.classify(msg)
    end

    test "accepts string ids" do
      msg = %{"method" => "transact", "params" => ["db"], "id" => "my-txn"}
      assert {:ok, {:request, %{id: "my-txn"}}} = Protocol.classify(msg)
    end
  end

  describe "classify/1 — forward compatibility" do
    test "ignores extra fields on requests" do
      msg = %{"method" => "x", "params" => [], "id" => 1, "future_field" => "extension"}
      assert {:ok, {:request, %{method: "x"}}} = Protocol.classify(msg)
    end
  end

  describe "classify/1 — errors" do
    test "rejects method-bearing messages without params" do
      assert {:error, {:missing_fields, _}} = Protocol.classify(%{"method" => "x"})
    end

    test "rejects method-bearing messages without id" do
      assert {:error, {:missing_fields, _}} = Protocol.classify(%{"method" => "x", "params" => []})
    end

    test "rejects responses with both result and error non-null" do
      msg = %{"result" => 1, "error" => "also_set", "id" => 1}
      assert {:error, :response_result_and_error_both_set} = Protocol.classify(msg)
    end

    test "rejects responses with both result and error null" do
      msg = %{"result" => nil, "error" => nil, "id" => 1}
      assert {:error, :response_result_and_error_both_null} = Protocol.classify(msg)
    end

    test "rejects responses with null id" do
      msg = %{"result" => 1, "error" => nil, "id" => nil}
      assert {:error, :response_id_null} = Protocol.classify(msg)
    end

    test "rejects empty maps" do
      assert {:error, :unclassifiable} = Protocol.classify(%{})
    end

    test "rejects non-map input" do
      assert {:error, {:not_a_map, _}} = Protocol.classify("not a map")
      assert {:error, {:not_a_map, _}} = Protocol.classify([1, 2])
      assert {:error, {:not_a_map, _}} = Protocol.classify(nil)
    end
  end

  describe "serialize/1" do
    test "produces iodata that decodes back to the original" do
      req = Protocol.request("list_dbs", [], 1)
      wire = req |> Protocol.serialize() |> IO.iodata_to_binary()
      assert {:ok, ^req} = Protocol.parse(wire)
    end

    test "serializes requests" do
      wire =
        Protocol.request("get_schema", ["Open_vSwitch"], 42)
        |> Protocol.serialize()
        |> IO.iodata_to_binary()

      {:ok, parsed} = Protocol.parse(wire)
      assert parsed["method"] == "get_schema"
      assert parsed["params"] == ["Open_vSwitch"]
      assert parsed["id"] == 42
    end

    test "serializes responses" do
      wire =
        Protocol.response(1, ["db_a", "db_b"])
        |> Protocol.serialize()
        |> IO.iodata_to_binary()

      {:ok, parsed} = Protocol.parse(wire)
      assert parsed["result"] == ["db_a", "db_b"]
      assert parsed["error"] == nil
      assert parsed["id"] == 1
    end

    test "serializes error responses" do
      wire =
        Protocol.error_response(1, "unknown database")
        |> Protocol.serialize()
        |> IO.iodata_to_binary()

      {:ok, parsed} = Protocol.parse(wire)
      assert parsed["error"] == "unknown database"
      assert parsed["result"] == nil
    end

    test "serializes notifications" do
      wire =
        Protocol.notification("echo", ["keepalive"])
        |> Protocol.serialize()
        |> IO.iodata_to_binary()

      {:ok, parsed} = Protocol.parse(wire)
      assert parsed["method"] == "echo"
      assert parsed["id"] == nil
    end
  end

  describe "parse/1" do
    test "parses valid JSON object" do
      assert {:ok, %{"method" => "list_dbs", "id" => 1}} =
               Protocol.parse(~s({"method":"list_dbs","params":[],"id":1}))
    end

    test "rejects malformed JSON" do
      assert {:error, {:parse_error, _}} = Protocol.parse("not json")
      assert {:error, {:parse_error, _}} = Protocol.parse("{ incomplete")
    end

    test "rejects non-object JSON values" do
      assert {:error, :not_an_object} = Protocol.parse("[1,2,3]")
      assert {:error, :not_an_object} = Protocol.parse("42")
      assert {:error, :not_an_object} = Protocol.parse(~s("string"))
      assert {:error, :not_an_object} = Protocol.parse("null")
    end
  end

  describe "parse_and_classify/1" do
    test "parses then classifies in one step" do
      wire = ~s({"method":"list_dbs","params":[],"id":1})
      assert {:ok, {:request, %{id: 1, method: "list_dbs"}}} = Protocol.parse_and_classify(wire)
    end

    test "propagates parse errors" do
      assert {:error, {:parse_error, _}} = Protocol.parse_and_classify("garbage")
    end

    test "propagates classify errors" do
      # valid JSON but malformed OVSDB message
      wire = ~s({"method":"x"})
      assert {:error, {:missing_fields, _}} = Protocol.parse_and_classify(wire)
    end
  end

  describe "full roundtrip — build → serialize → parse → classify" do
    test "requests roundtrip" do
      for {method, params, id} <- [
            {"list_dbs", [], 1},
            {"get_schema", ["Open_vSwitch"], 2},
            {"transact", ["db", %{"op" => "select", "table" => "Bridge"}], "txn-3"}
          ] do
        msg = Protocol.request(method, params, id)
        wire = msg |> Protocol.serialize() |> IO.iodata_to_binary()
        {:ok, {:request, classified}} = Protocol.parse_and_classify(wire)

        assert classified.method == method
        assert classified.params == params
        assert classified.id == id
      end
    end

    test "responses roundtrip" do
      msg = Protocol.response(1, ["a", "b"])
      wire = msg |> Protocol.serialize() |> IO.iodata_to_binary()

      assert {:ok, {:response, %{id: 1, result: ["a", "b"], error: nil}}} =
               Protocol.parse_and_classify(wire)
    end

    test "error responses roundtrip with nested error+details" do
      msg = Protocol.error_response(42, "syntax error", "unexpected token at byte 42")
      wire = msg |> Protocol.serialize() |> IO.iodata_to_binary()

      {:ok, {:response, %{id: 42, result: nil, error: error}}} =
        Protocol.parse_and_classify(wire)

      assert error == %{
               "error" => "syntax error",
               "details" => "unexpected token at byte 42"
             }
    end

    test "notifications roundtrip" do
      msg = Protocol.notification("update", ["monitor-id", %{"Bridge" => %{}}])
      wire = msg |> Protocol.serialize() |> IO.iodata_to_binary()

      assert {:ok, {:notification, %{method: "update", params: params}}} =
               Protocol.parse_and_classify(wire)

      assert params == ["monitor-id", %{"Bridge" => %{}}]
    end
  end
end
