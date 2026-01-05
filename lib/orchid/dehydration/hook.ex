defmodule Orchid.Dehydration.Hook.Threshold do
  @moduledoc false

  defguard is_big(payload, binary_size, enumerable_size)
           when (is_binary(payload) and byte_size(payload) >= binary_size) or
                  (is_list(payload) and length(payload) >= enumerable_size) or
                  (is_map(payload) and map_size(payload) >= enumerable_size)
end

defmodule Orchid.Dehydration.Hook do
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
  import Orchid.Dehydration.Hook.Threshold

  @behaviour Orchid.Runner.Hook

  @binary_size_threshold 1_024

  def call(ctx, next) do
    with {:ok, hydrated} <- hydrate_all(ctx.inputs),
         {:ok, raw_output} <- next.(%{ctx | inputs: hydrated}) do
      {repo, opts} = get_repo_and_opts(ctx)

      raw_output
      |> dehydrate_all(repo, get_threshold(ctx), opts)
    else
      error_or_special -> error_or_special
    end
  end

  defp hydrate_all(inputs) when is_list(inputs) do
    Enum.reduce_while(inputs, [], fn param, acc ->
      case hydrate_param(param) do
        {:ok, p} -> {:cont, [p | acc]}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:error, _} = err -> err
      list when is_list(list) -> {:ok, Enum.reverse(list)}
    end
  end

  defp hydrate_all(input), do: hydrate_param(input)

  defp hydrate_param(%Orchid.Param{payload: {:ref, repo, key}} = param)
       when is_atom(repo) do
    case repo.get(key) do
      {:ok, data} -> {:ok, %{param | payload: data}}
      {:error, reason} -> {:error, {:hydrate_failed, param.name, reason}}
    end
  end

  defp hydrate_param(param), do: {:ok, param}

  defp dehydrate_all(outputs, repo, threshold, opts) when is_list(outputs) do
    Enum.reduce_while(outputs, [], fn param, acc ->
      case dehydrate_param(param, repo, threshold, opts) do
        {:ok, p} -> {:cont, [p | acc]}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:error, _} = err -> err
      list when is_list(list) -> {:ok, Enum.reverse(list)}
    end
  end

  defp dehydrate_all(output, repo, threshold, opts) do
    dehydrate_param(output, repo, threshold, opts)
  end

  defp dehydrate_param(%Orchid.Param{payload: raw_data} = param, repo, threshold, opts)
       when is_big(raw_data, threshold, div(threshold, 8)) do
    case repo.put(raw_data, opts) do
      {:ok, result} ->
        ref_payload = {:ref, repo, result}
        {:ok, %{param | payload: ref_payload}}

      {:error, reason} ->
        {:error, {:dehydrate_failed, param.name, reason}}
    end
  end

  defp dehydrate_param(param, _repo, _threshold, _opts), do: {:ok, param}

  defp get_repo_and_opts(ctx) do
    Orchid.WorkflowCtx.get_baggage(
      ctx.workflow_ctx,
      :repo_and_opts,
      {Orchid.Repos.EtsAdapter, []}
    )
  end

  defp get_threshold(ctx) do
    Orchid.WorkflowCtx.get_baggage(ctx.workflow_ctx, :dehydrate_threshold, @binary_size_threshold)
  end
end
