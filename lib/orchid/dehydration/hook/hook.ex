defmodule Orchid.Dehydration.Hook.Param do
  @moduledoc """
  Runner Hook that handles transparent data offloading (Dehydration) and reloading (Hydration).

  ## Configuration (via workflow_ctx baggage)

    * `:repo_and_opts` - Repo module to use (and opts). Defaults to `{Orchid.Repos.EtsAdapter, []}`.
    * `:dehydrate_threshold` - Size in bytes. Defaults to 1024 bytes.

  ## Usage

      Orchid.run(recipe, inputs,
        global_hooks_stack: [Orchid.Dehydration.Hook, ...]
      )
  """
  alias Orchid.{Param, WorkflowCtx}
  alias Orchid.Runner.{Context, Hook}

  @behaviour Hook

  @binary_size_threshold 1_024

  @spec call(Context.t(), Hook.next_fn()) :: Hook.hook_result()
  def call(ctx, next) do
    with {:ok, hydrated} <- ctx.inputs |> List.wrap() |> hydrate_params(),
         {:ok, raw_output} <- next.(%{ctx | inputs: hydrated}) do
      {repo, opts} = get_repo_and_opts(ctx)

      raw_output
      |> List.wrap()
      |> dehydrate_params(repo, get_threshold(ctx), opts)
    else
      error_or_special -> error_or_special
    end
  end

  # ==== Hydrate ====

  defp hydrate_params(inputs) do
    Enum.reduce_while(inputs, [], fn param, acc ->
      case hydrate_payload(param) do
        {:ok, p} -> {:cont, [p | acc]}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:error, _} = err -> err
      [param] -> {:ok, param}
      params -> {:ok, Enum.reverse(params)}
    end
  end

  defp hydrate_payload(%Param{payload: {:ref, repo, key}} = param)
       when is_atom(repo) do
    case repo.get(key) do
      {:ok, data} -> {:ok, Param.set_payload(param, data)}
      {:error, reason} -> {:error, {:hydrate_failed, param.name, reason}}
    end
  end

  defp hydrate_payload(param), do: {:ok, param}

  # ==== Dehydrate ====

  defp dehydrate_params(outputs, repo, threshold, opts) do
    Enum.reduce_while(outputs, [], fn param, acc ->
      case dehydrate_payload(param, repo, threshold, opts) do
        {:ok, p} -> {:cont, [p | acc]}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:error, _} = err -> err
      [param] -> {:ok, param}
      params -> {:ok, Enum.reverse(params)}
    end
  end

  defp dehydrate_payload(%Param{payload: raw_data} = param, repo, threshold, opts) do
    if should_dehydrate?(raw_data, threshold) do
      case repo.put(raw_data, opts) do
        {:ok, result} ->
          ref_payload = {:ref, repo, result}
          {:ok, Param.set_payload(param, ref_payload)}

        {:error, reason} ->
          {:error, {:dehydrate_failed, param.name, reason}}
      end
    else
      {:ok, param}
    end
  end

  # Adapt Nx
  defp should_dehydrate?(%{shape: shape, type: {_type, bits_per_element}}, threshold)
       when is_tuple(shape) and is_number(bits_per_element),
       do: (Tuple.product(shape) * (bits_per_element / 8)) > threshold

  defp should_dehydrate?(data, threshold) when is_binary(data), do: byte_size(data) > threshold
  defp should_dehydrate?(data, threshold) when is_list(data), do: length(data) > div(threshold, 8)
  defp should_dehydrate?(data, threshold) when is_map(data), do: map_size(data) > div(threshold, 16)
  defp should_dehydrate?(data, threshold), do: :erlang.external_size(data) >= 2 * threshold

  # ==== Option Fetch ====

  defp get_repo_and_opts(ctx) do
    WorkflowCtx.get_baggage(
      ctx.workflow_ctx,
      :repo_and_opts,
      {Orchid.Repos.EtsAdapter, []}
    )
  end

  defp get_threshold(ctx) do
    WorkflowCtx.get_baggage(ctx.workflow_ctx, :dehydrate_threshold, @binary_size_threshold)
  end
end
