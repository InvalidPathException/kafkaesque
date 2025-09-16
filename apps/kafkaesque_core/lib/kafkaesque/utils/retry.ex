defmodule Kafkaesque.Utils.Retry do
  @moduledoc """
  Utilities for retrying operations with exponential backoff.
  """

  require Logger

  @doc """
  Execute a function with exponential backoff retry logic.

  ## Options
    - `:max_retries` - Maximum number of retry attempts (default: 3)
    - `:initial_delay` - Initial delay in milliseconds (default: 100)
    - `:max_delay` - Maximum delay between retries in milliseconds (default: 5000)
    - `:backoff_factor` - Factor to multiply delay by on each retry (default: 2)
    - `:jitter` - Whether to add random jitter to delays (default: true)
    - `:retry_on` - List of error patterns to retry on (default: [:all])

  ## Examples

      # Retry any error up to 3 times
      retry_with_backoff(fn -> do_something() end)

      # Retry specific errors with custom settings
      retry_with_backoff(
        fn -> connect_to_service() end,
        max_retries: 5,
        initial_delay: 200,
        retry_on: [:timeout, :connection_refused]
      )
  """
  def retry_with_backoff(func, opts \\ []) when is_function(func, 0) do
    max_retries = Keyword.get(opts, :max_retries, 3)
    initial_delay = Keyword.get(opts, :initial_delay, 100)
    max_delay = Keyword.get(opts, :max_delay, 5000)
    backoff_factor = Keyword.get(opts, :backoff_factor, 2)
    jitter = Keyword.get(opts, :jitter, true)
    retry_on = Keyword.get(opts, :retry_on, [:all])

    do_retry(func, 0, max_retries, initial_delay, max_delay, backoff_factor, jitter, retry_on)
  end

  defp do_retry(func, attempt, max_retries, delay, max_delay, backoff_factor, jitter, retry_on) do
    case func.() do
      {:ok, result} ->
        {:ok, result}

      {:error, reason} = error ->
        if should_retry?(reason, retry_on) and attempt < max_retries do
          actual_delay = calculate_delay(delay, jitter)

          Logger.debug(
            "Retry attempt #{attempt + 1}/#{max_retries} after #{actual_delay}ms. Error: #{inspect(reason)}"
          )

          Process.sleep(actual_delay)

          next_delay = min(delay * backoff_factor, max_delay)

          do_retry(
            func,
            attempt + 1,
            max_retries,
            next_delay,
            max_delay,
            backoff_factor,
            jitter,
            retry_on
          )
        else
          if attempt >= max_retries do
            Logger.warning(
              "Max retries (#{max_retries}) exceeded. Final error: #{inspect(reason)}"
            )
          end

          error
        end

      other ->
        # Handle non-standard return values
        other
    end
  rescue
    exception ->
      if attempt < max_retries do
        actual_delay = calculate_delay(delay, jitter)

        Logger.debug(
          "Retry attempt #{attempt + 1}/#{max_retries} after #{actual_delay}ms. Exception: #{inspect(exception)}"
        )

        Process.sleep(actual_delay)

        next_delay = min(delay * backoff_factor, max_delay)

        do_retry(
          func,
          attempt + 1,
          max_retries,
          next_delay,
          max_delay,
          backoff_factor,
          jitter,
          retry_on
        )
      else
        Logger.warning(
          "Max retries (#{max_retries}) exceeded. Final exception: #{inspect(exception)}"
        )

        reraise exception, __STACKTRACE__
      end
  end

  defp should_retry?(_reason, [:all]), do: true

  defp should_retry?(reason, patterns) when is_list(patterns) do
    Enum.any?(patterns, fn pattern ->
      case pattern do
        atom when is_atom(atom) ->
          reason == atom

        func when is_function(func, 1) ->
          func.(reason)

        _ ->
          false
      end
    end)
  end

  defp calculate_delay(base_delay, false), do: base_delay

  defp calculate_delay(base_delay, true) do
    # Add up to 25% jitter
    jitter_range = div(base_delay, 4)
    base_delay + :rand.uniform(jitter_range)
  end

  defmodule CircuitBreaker do
    @moduledoc """
    Create a circuit breaker for a function.

    The circuit breaker tracks failures and temporarily stops calling the function
    if too many failures occur.
    """

    use GenServer

    defstruct [
      :name,
      :func,
      :failure_threshold,
      :success_threshold,
      :timeout,
      :state,
      :failure_count,
      :success_count,
      :last_failure_time
    ]

    def start_link(opts) do
      name = Keyword.fetch!(opts, :name)
      GenServer.start_link(__MODULE__, opts, name: name)
    end

    def init(opts) do
      state = %__MODULE__{
        name: Keyword.fetch!(opts, :name),
        func: Keyword.fetch!(opts, :func),
        failure_threshold: Keyword.get(opts, :failure_threshold, 5),
        success_threshold: Keyword.get(opts, :success_threshold, 2),
        timeout: Keyword.get(opts, :timeout, 60_000),
        state: :closed,
        failure_count: 0,
        success_count: 0,
        last_failure_time: nil
      }

      {:ok, state}
    end

    def call(name, args \\ []) do
      GenServer.call(name, {:call, args})
    end

    def handle_call({:call, args}, _from, state) do
      case state.state do
        :open ->
          if should_attempt?(state) do
            # Try half-open
            new_state = %{state | state: :half_open, success_count: 0}
            execute_function(new_state, args)
          else
            {:reply, {:error, :circuit_open}, state}
          end

        :half_open ->
          execute_function(state, args)

        :closed ->
          execute_function(state, args)
      end
    end

    defp should_attempt?(%{last_failure_time: nil}), do: false

    defp should_attempt?(%{last_failure_time: last_failure, timeout: timeout}) do
      System.monotonic_time(:millisecond) - last_failure >= timeout
    end

    defp execute_function(state, args) do
      result = apply(state.func, args)

      case result do
        {:ok, _} = success ->
          new_state = handle_success(state)
          {:reply, success, new_state}

        {:error, _} = error ->
          new_state = handle_failure(state)
          {:reply, error, new_state}

        other ->
          new_state = handle_success(state)
          {:reply, {:ok, other}, new_state}
      end
    rescue
      exception ->
        new_state = handle_failure(state)
        {:reply, {:error, exception}, new_state}
    end

    defp handle_success(state) do
      case state.state do
        :half_open ->
          new_success_count = state.success_count + 1

          if new_success_count >= state.success_threshold do
            Logger.info("Circuit breaker #{state.name} closed after successful recovery")
            %{state | state: :closed, failure_count: 0, success_count: 0}
          else
            %{state | success_count: new_success_count}
          end

        _ ->
          %{state | failure_count: 0}
      end
    end

    defp handle_failure(state) do
      new_failure_count = state.failure_count + 1
      now = System.monotonic_time(:millisecond)

      case state.state do
        :half_open ->
          Logger.warning(
            "Circuit breaker #{state.name} reopened after failure in half-open state"
          )

          %{state | state: :open, failure_count: new_failure_count, last_failure_time: now}

        :closed when new_failure_count >= state.failure_threshold ->
          Logger.warning(
            "Circuit breaker #{state.name} opened after #{new_failure_count} failures"
          )

          %{state | state: :open, failure_count: new_failure_count, last_failure_time: now}

        _ ->
          %{state | failure_count: new_failure_count, last_failure_time: now}
      end
    end
  end
end
