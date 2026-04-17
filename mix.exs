defmodule OVSDB.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/HeroesLament/ovsdb_ex"

  def project do
    [
      app: :ovsdb_ex,
      version: @version,
      elixir: "~> 1.15",
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

      # Dialyzer
      dialyzer: [
        plt_add_apps: [:mix, :ex_unit],
        flags: [:error_handling, :underspecs, :unmatched_returns]
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

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
    Protocol (OVSDB), per RFC 7047. Provides a client (IDL-style in-memory
    replica) and a server (accept connections and handle RPCs) for any
    application that needs to speak OVSDB.
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
        ]
      ]
    ]
  end
end
