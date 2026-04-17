defmodule OVSDB.MixProject do
  use Mix.Project

  @version "0.2.0"
  @source_url "https://github.com/HeroesLament/ovsdb_ex"

  def project do
    [
      app: :ovsdb_ex,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # Hex metadata
      description: description(),
      package: package(),

      # HexDocs metadata
      name: "OVSDB",
      source_url: @source_url,
      homepage_url: @source_url,
      docs: docs(),

      # Quiet xref for :ssl — TLS is an optional transport and always
      # available as part of OTP, so cross-reference checks on its
      # functions aren't useful.
      xref: [exclude: [:ssl]],

      # Dialyzer
      dialyzer: [
        plt_add_apps: [:mix, :ex_unit, :ssl, :crypto],
        flags: [:error_handling, :underspecs, :unmatched_returns]
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger, :ssl, :crypto]
    ]
  end

  # Compile test/support modules only when running tests. Keeps the
  # hex package slim and avoids bundling test-only handlers.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # Runtime
      {:jason, "~> 1.4"},

      # Development & documentation
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false}
    ]
  end

  defp description do
    """
    A pure-Elixir implementation of the Open vSwitch Database Management
    Protocol (OVSDB), per RFC 7047. Provides protocol primitives, operation
    and transaction builders, schema parsing, client/server session
    handling over TCP/TLS, and an in-memory IDL replica.
    """
  end

  defp package do
    [
      name: "ovsdb_ex",
      maintainers: ["HeroesLament <nunya@biz.net>"],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md",
        "RFC 7047" => "https://www.rfc-editor.org/rfc/rfc7047"
      },
      files: ~w(
        lib
        .formatter.exs
        mix.exs
        README.md
        LICENSE
        CHANGELOG.md
      )
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: [
        "README.md",
        "CHANGELOG.md": [title: "Changelog"],
        LICENSE: [title: "License"]
      ],
      source_ref: "v#{@version}",
      groups_for_modules: [
        "Data Model": [
          OVSDB.UUID,
          OVSDB.NamedUUID,
          OVSDB.Set,
          OVSDB.Map,
          OVSDB.Row,
          OVSDB.Value
        ],
        Protocol: [
          OVSDB.Protocol
        ],
        "Operations & Transactions": [
          OVSDB.Condition,
          OVSDB.Operation,
          OVSDB.Transaction,
          OVSDB.MonitorSpec
        ],
        Schema: [
          OVSDB.Schema,
          OVSDB.Schema.Column,
          OVSDB.Schema.Table,
          OVSDB.SchemaHelper
        ],
        "Sessions & Transport": [
          OVSDB.Framer,
          OVSDB.Transport,
          OVSDB.ClientSession,
          OVSDB.ServerSession,
          OVSDB.ServerSession.Handler,
          OVSDB.Server
        ],
        IDL: [
          OVSDB.Idl
        ]
      ]
    ]
  end
end
