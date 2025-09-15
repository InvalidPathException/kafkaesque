defmodule Kafkaesque.HealthTest do
  use ExUnit.Case, async: false

  alias Kafkaesque.Health

  setup do
    # Ensure required processes are started
    unless Process.whereis(Kafkaesque.TopicRegistry) do
      {:ok, _} = Registry.start_link(keys: :unique, name: Kafkaesque.TopicRegistry)
    end

    unless Process.whereis(Kafkaesque.Telemetry) do
      {:ok, _} = Kafkaesque.Telemetry.start_link()
    end

    unless Process.whereis(Kafkaesque.Topic.Supervisor) do
      {:ok, _} = Kafkaesque.Topic.Supervisor.start_link([])
    end

    # Ensure the data directory exists for health checks
    data_dir = Application.get_env(:kafkaesque_core, :data_dir, "./data")
    File.mkdir_p!(data_dir)

    on_exit(fn ->
      # Clean up test data directory if it was created
      if data_dir == "./data" do
        File.rm_rf!(data_dir)
      end
    end)

    :ok
  end

  describe "check/0" do
    test "returns healthy status when all checks pass" do
      assert {:ok, result} = Health.check()
      assert result.status == :healthy
      assert is_integer(result.timestamp)
      assert is_list(result.checks)
      assert length(result.checks) > 0
    end

    test "includes all required checks" do
      {:ok, result} = Health.check()

      check_names = Enum.map(result.checks, & &1.name)
      assert :registry in check_names
      assert :topics in check_names
      assert :telemetry in check_names
      assert :disk in check_names
      assert :memory in check_names
    end
  end

  describe "alive?/0" do
    test "returns :ok" do
      assert Health.alive?() == :ok
    end
  end

  describe "ready?/0" do
    test "returns true when system is healthy" do
      assert Health.ready?() == true
    end
  end

  describe "info/0" do
    test "returns system information" do
      info = Health.info()

      assert info.version == "0.1.0"
      assert is_atom(info.node)
      assert is_integer(info.uptime_ms)
      assert is_integer(info.schedulers)
      assert is_binary(info.otp_release)
      assert is_binary(info.elixir_version)
    end
  end
end
