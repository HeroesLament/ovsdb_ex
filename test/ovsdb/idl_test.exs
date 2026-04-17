defmodule OVSDB.IdlTest do
  use ExUnit.Case, async: true

  alias OVSDB.{
    ClientSession,
    Condition,
    Idl,
    Operation,
    Schema,
    SchemaHelper,
    Server,
    Transaction,
    UUID
  }

  alias OVSDB.TestSupport.IdlHandler

  # Each test gets its own server + session + idl trio.
  setup do
    {:ok, srv} = Server.start_link(port: 0, handler: IdlHandler)
    port = Server.listen_port(srv)

    {:ok, session} = ClientSession.connect("127.0.0.1", port)
    {:ok, schema_json} = ClientSession.get_schema(session, "Test")
    {:ok, schema} = Schema.parse(schema_json)
    helper = SchemaHelper.new(schema) |> SchemaHelper.register_table!("Test_Bridge")

    {:ok, idl} =
      Idl.start_link(session: session, helper: helper, monitor_id: "test-idl")

    Idl.subscribe(idl, "Test_Bridge")

    on_exit(fn ->
      for pid <- [idl, session, srv], Process.alive?(pid) do
        try do
          cond do
            pid == idl -> Idl.stop(idl)
            pid == session -> ClientSession.close(session)
            pid == srv -> Server.stop(srv)
          end
        catch
          :exit, _ -> :ok
        end
      end
    end)

    %{srv: srv, session: session, idl: idl}
  end

  # Helper: perform an insert and return the uuid that came back.
  defp insert_row(session, row) do
    txn =
      Transaction.new("Test")
      |> Transaction.add(Operation.insert("Test_Bridge", row))

    {:ok, [%{"uuid" => ["uuid", uuid]}]} = ClientSession.transact(session, txn)
    # Let the notification propagate
    Process.sleep(50)
    uuid
  end

  describe "initial state" do
    test "empty replica before any transacts", %{idl: idl} do
      assert Idl.get_table(idl, "Test_Bridge") == %{}
      assert Idl.change_seqno(idl) == 0
    end
  end

  describe "insert" do
    test "row appears in the IDL after transact", %{session: session, idl: idl} do
      uuid = insert_row(session, %{"name" => "br-1", "count" => 1})

      rows = Idl.get_table(idl, "Test_Bridge")
      assert Elixir.Map.has_key?(rows, uuid)
      assert rows[uuid]["name"] == "br-1"
      assert rows[uuid]["count"] == 1
    end

    test "seqno bumps on insert", %{session: session, idl: idl} do
      _uuid = insert_row(session, %{"name" => "br-1", "count" => 1})
      assert Idl.change_seqno(idl) == 1
    end

    test "subscribers receive :insert notification", %{session: session, idl: idl} do
      uuid = insert_row(session, %{"name" => "br-1", "count" => 1})
      assert_receive {:idl_changed, ^idl, "Test_Bridge", :insert, ^uuid}, 1_000
    end
  end

  describe "modify — partial column merge" do
    test "changed columns are updated, others preserved", %{session: session, idl: idl} do
      uuid = insert_row(session, %{"name" => "br-1", "count" => 1})
      # Drain the insert notification so we can assert the modify one
      assert_receive {:idl_changed, ^idl, "Test_Bridge", :insert, ^uuid}, 1_000

      # Modify only the count
      txn =
        Transaction.new("Test")
        |> Transaction.add(
          Operation.update(
            "Test_Bridge",
            [Condition.eq("_uuid", UUID.new(uuid))],
            %{"count" => 42}
          )
        )

      {:ok, [%{"count" => 1}]} = ClientSession.transact(session, txn)
      Process.sleep(50)

      # name should still be present — this is the critical merge test
      assert {:ok, %{"name" => "br-1", "count" => 42}} = Idl.get_row(idl, "Test_Bridge", uuid)

      assert Idl.change_seqno(idl) == 2
      assert_receive {:idl_changed, ^idl, "Test_Bridge", :modify, ^uuid}, 1_000
    end
  end

  describe "delete" do
    test "row disappears from the replica", %{session: session, idl: idl} do
      uuid = insert_row(session, %{"name" => "to-delete", "count" => 0})
      assert_receive {:idl_changed, ^idl, "Test_Bridge", :insert, ^uuid}, 1_000

      txn =
        Transaction.new("Test")
        |> Transaction.add(
          Operation.delete("Test_Bridge", [Condition.eq("_uuid", UUID.new(uuid))])
        )

      {:ok, [%{"count" => 1}]} = ClientSession.transact(session, txn)
      Process.sleep(50)

      assert :error = Idl.get_row(idl, "Test_Bridge", uuid)
      assert Idl.get_table(idl, "Test_Bridge") == %{}

      assert_receive {:idl_changed, ^idl, "Test_Bridge", :delete, ^uuid}, 1_000
    end
  end

  describe "sequence of operations" do
    test "seqno increases monotonically", %{session: session, idl: idl} do
      assert Idl.change_seqno(idl) == 0

      uuid1 = insert_row(session, %{"name" => "a", "count" => 1})
      seqno_after_insert = Idl.change_seqno(idl)
      assert seqno_after_insert >= 1

      # Drain notification
      receive do
        {:idl_changed, _, _, :insert, ^uuid1} -> :ok
      after
        1_000 -> flunk("no insert notification")
      end

      uuid2 = insert_row(session, %{"name" => "b", "count" => 2})
      seqno_after_second_insert = Idl.change_seqno(idl)

      assert seqno_after_second_insert > seqno_after_insert

      # Drain
      receive do
        {:idl_changed, _, _, :insert, ^uuid2} -> :ok
      after
        1_000 -> flunk("no second insert notification")
      end
    end
  end

  describe "multiple tables are isolated" do
    test "only registered tables appear", %{idl: idl} do
      # Helper was only registered for "Test_Bridge"; any other table
      # shouldn't appear even if the schema has one. Our test schema
      # only has Test_Bridge, so this mostly verifies the happy case.
      assert Idl.get_table(idl, "Test_Bridge") == %{}
      # Nonexistent table returns empty map from ETS read
      assert Idl.get_table(idl, "Totally_Fake_Table") == %{}
    end
  end

  describe "cached read API" do
    test "table_ids/1 returns the two ETS handles", %{idl: idl} do
      {rows_ets, meta_ets} = Idl.table_ids(idl)
      assert is_reference(rows_ets) or is_atom(rows_ets) or is_integer(rows_ets)
      assert is_reference(meta_ets) or is_atom(meta_ets) or is_integer(meta_ets)
    end

    test "get_table_cached matches get_table", %{session: session, idl: idl} do
      _uuid = insert_row(session, %{"name" => "x", "count" => 1})
      {rows_ets, _} = Idl.table_ids(idl)

      assert Idl.get_table_cached(rows_ets, "Test_Bridge") ==
               Idl.get_table(idl, "Test_Bridge")
    end

    test "change_seqno_cached matches change_seqno", %{session: session, idl: idl} do
      _uuid = insert_row(session, %{"name" => "x", "count" => 1})
      {_, meta_ets} = Idl.table_ids(idl)

      assert Idl.change_seqno_cached(meta_ets) == Idl.change_seqno(idl)
    end
  end
end
