using System.Net.Sockets;
using System.Text;
using EventBus.Base;
using EventBus.Base.Events;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace EventBus.RabbitMQ;

public class EventBusServiceBus : BaseEventBus
{
    private readonly RabbitMQPersistentConnection _rabbitMQPersistentConnection;
    private readonly IConnectionFactory _connectionFactory;
    private readonly IModel _consumerChannel;

    public EventBusServiceBus(EventBusConfig eventBusConfig, IServiceProvider serviceProvider)
        : base(eventBusConfig, serviceProvider)
    {
        if (eventBusConfig.Connection is not null)
        {
            string connectionStr = JsonConvert.SerializeObject(
                EventBusConfig.Connection,
                new JsonSerializerSettings { ReferenceLoopHandling = ReferenceLoopHandling.Ignore }
            );

            _connectionFactory = JsonConvert.DeserializeObject<ConnectionFactory>(connectionStr)!;
        }
        else
            _connectionFactory = new ConnectionFactory();

        _rabbitMQPersistentConnection = new(_connectionFactory, eventBusConfig.ConnectionRetryCount);

        _consumerChannel = CreateConsumerChannel();

        EventBusSubscriptionManager.OnEventRemoved += EventBusSubscriptionManager_OnEventRemoved;
    }

    private void EventBusSubscriptionManager_OnEventRemoved(object? sender, string e)
    {
        e = ProcessEventName(e);

        if (!_rabbitMQPersistentConnection.IsConnected)
            _rabbitMQPersistentConnection.TryConnect();

        _consumerChannel.QueueUnbind(queue: e, exchange: EventBusConfig.DefaultTopicName, routingKey: e);

        if (EventBusSubscriptionManager.IsEmpty)
            _consumerChannel.Close();
    }

    public override void Publish(IntegrationEvent @event)
    {
        if (!_rabbitMQPersistentConnection.IsConnected)
            _rabbitMQPersistentConnection.TryConnect();

        RetryPolicy policy = Policy
            .Handle<BrokerUnreachableException>()
            .Or<SocketException>()
            .WaitAndRetry(
                EventBusConfig.ConnectionRetryCount,
                retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                (ex, time) => { }
            );

        string eventName = @event.GetType().Name;

        eventName = ProcessEventName(eventName);

        _consumerChannel.ExchangeDeclare(exchange: EventBusConfig.DefaultTopicName, type: "direct");

        string message = JsonConvert.SerializeObject(@event);
        byte[] body = Encoding.UTF8.GetBytes(message);

        policy.Execute(() =>
        {
            IBasicProperties properties = _consumerChannel.CreateBasicProperties();
            properties.DeliveryMode = 2;

            _consumerChannel.QueueDeclare(
                queue: GetSubName(eventName),
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null
            );

            _consumerChannel.BasicPublish(
                exchange: EventBusConfig.DefaultTopicName,
                routingKey: eventName,
                mandatory: true,
                basicProperties: properties,
                body: body
            );
        });
    }

    public override void Subscribe<T, TH>()
    {
        string eventName = typeof(T).Name;

        eventName = ProcessEventName(eventName);

        if (!EventBusSubscriptionManager.HasSubscriptionsForEvent(eventName))
        {
            if (!_rabbitMQPersistentConnection.IsConnected)
                _rabbitMQPersistentConnection.TryConnect();

            _consumerChannel.QueueDeclare(
                queue: GetSubName(eventName),
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null
            );

            _consumerChannel.QueueBind(
                queue: GetSubName(eventName),
                exchange: EventBusConfig.DefaultTopicName,
                routingKey: eventName
            );
        }

        EventBusSubscriptionManager.AddSubscription<T, TH>();
        StartBasicConsume(eventName);
    }

    public override void Unsubscribe<T, TH>()
    {
        EventBusSubscriptionManager.RemoveSubscription<T, TH>();
    }

    private IModel CreateConsumerChannel()
    {
        if (!_rabbitMQPersistentConnection.IsConnected)
            _rabbitMQPersistentConnection.TryConnect();

        IModel channel = _rabbitMQPersistentConnection.CreateModel();

        channel.ExchangeDeclare(exchange: EventBusConfig.DefaultTopicName, type: "direct");

        return channel;
    }

    private void StartBasicConsume(string eventName)
    {
        if (_consumerChannel is not null)
        {
            EventingBasicConsumer consumer = new(_consumerChannel);

            consumer.Received += Consumer_Receiver;

            _consumerChannel.BasicConsume(queue: GetSubName(eventName), autoAck: false, consumer: consumer);
        }
    }

    private async void Consumer_Receiver(object? sender, BasicDeliverEventArgs e)
    {
        string eventName = e.RoutingKey;

        eventName = ProcessEventName(eventName);

        string message = Encoding.UTF8.GetString(e.Body.Span);

        try
        {
            await ProcessEvent(eventName, message);
        }
        catch (Exception) { }

        _consumerChannel.BasicAck(e.DeliveryTag, multiple: false);
    }
}
