defmodule ExamqpHelpers do
  use GenServer
  use AMQP

  require Logger

  defp conn_string() do
    rconf = Application.get_env(:api, :rabbitmq)
    case rconf[:user] do
      nil -> "amqp://#{rconf[:host]}"
      _   -> "amqp://#{rconf[:user]}:#{rconf[:password]}@#{rconf[:host]}"
    end
  end

  def start_link, do: GenServer.start_link(__MODULE__, [], [])

  def init(_), do: connect()

  # TODO: Reconnect logic.
  def connect() do
    case Connection.open(conn_string()) do
      {:ok, conn} ->
        Logger.info "Connected to RabbitMQ."
        Process.monitor(conn.pid)
        {:ok, chan} = Channel.open(conn)
        {:ok, %{conn: conn, chan: chan}}
      {:error, _} ->
        :timer.sleep(5000)
        # Wait and reconnect.
        Logger.info "Attempting to reconnect to RabbitMQ..."
        connect()
    end
  end

  def handle_info({:DOWN, _, :process, _pid, _reason}, _) do
    {:ok, state} = connect()
    {:noreply, state}
  end

  # Get Message
  def get_message_autoack(queue) do
    with {:ok, message, ack_fn} <- get_message(queue),
         :ok <- ack_fn.(),
      do: {:ok, message}
  end

  def get_message(queue),
    do: GenServer.call(:queue_manager, {:get_message, queue})

  def handle_call({:get_message, queue}, _from, %{chan: chan}=state) do
    case Basic.get(chan, queue) do
      {:ok, message, meta} ->
        ack_fn = fn -> Basic.ack(chan, meta.delivery_tag) end
        {:reply, {:ok, message, meta.delivery_tag}, state}
      _ ->
        {:reply, :empty, state}
    end
  end

  # Get Queue Size
  def get_queue_size(queue) do
    count = GenServer.call(:queue_manager, {:get_queue_size, queue})
    {:ok, count}
  end

  def handle_call({:get_queue_size, queue}, _from, %{chan: chan}=state) do
    case Queue.declare(chan, queue, durable: true) do
      {:ok, %{message_count: c}} ->
        {:reply, c, state}
      _ ->
        {:reply, 0, state}
    end
  end

  # Publish Message
  def publish_message(queue, message),
    do: GenServer.call(:queue_manager, {:publish_message, queue, message})

  def handle_call({:publish_message, queue, message}, _from, %{chan: chan}=state) do
    exchange = queue <> "exchange"
    {:ok, _} = Queue.declare(chan, queue, durable: true)
    :ok = Exchange.fanout(chan, exchange, durable: true)
    :ok = Queue.bind(chan, queue, exchange)
    :ok = Basic.publish(chan, exchange, "", message)
    {:reply, :ok, state}
  end

  # Purge queue
  def purge_queue(queue),
    do: GenServer.call(:queue_manager, {:purge_queue, queue})

  def handle_call({:purge_queue, queue}, _from, %{chan: chan}=state) do
    case Queue.purge(chan, queue) do
      {:ok, %{message_count: _}} -> {:reply, :ok, state}
      _ -> {:reply, :error, state}
    end
  end
end
