defmodule Orchid.Repos.EtsAdapter do
  @behaviour Orchid.Repo
  use GenServer

  @table __MODULE__.Table

  # ===================================================================
  # Client API (Orchid Repo Contract)
  # ===================================================================

  @doc """
  Stores data and returns a reference key.

  Conforms to Orchid.Dehydration.Hook expectation: put(data, opts)
  """
  @impl true
  def put(data, _opts \\ []) do
    key = :erlang.phash2(data)

    try do
      :ets.insert(@table, {key, data})
      {:ok, key}
    rescue
      e -> {:error, e}
    end
  end

  @doc """
  Retrieves data by reference key.

  Conforms to Orchid.Dehydration.Hook expectation: get(key)
  """
  @impl true
  def get(key) do
    case :ets.lookup(@table, key) do
      [{^key, data}] -> {:ok, data}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Deletes data by reference key.

  Conforms to Orchid.Dehydration.Hook expectation: delete(key)
  """
  @impl true
  def delete(key) do
    :ets.delete(@table, key)

    :ok
  end

  # ===================================================================
  # GenServer Callbacks (Lifecycle)
  # ===================================================================

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])

    {:ok, %{}}
  end
end
