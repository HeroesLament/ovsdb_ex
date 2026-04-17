defmodule OVSDBTest do
  use ExUnit.Case, async: true

  test "version/0 returns the current library version" do
    assert is_binary(OVSDB.version())
    assert OVSDB.version() =~ ~r/\A\d+\.\d+\.\d+/
  end
end
