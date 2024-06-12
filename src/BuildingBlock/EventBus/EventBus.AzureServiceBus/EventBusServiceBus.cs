using System.Text;
using EventBus.Base;
using EventBus.Base.Events;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace EventBus.AzureServiceBus;

public class EventBusServiceBus : BaseEventBus
{
    private ITopicClient _topicClient;
    private ManagementClient _managementClient;
    private readonly ILogger _logger;

    public EventBusServiceBus(EventBusConfig eventBusConfig, IServiceProvider serviceProvider)
        : base(eventBusConfig, serviceProvider)
    {
        _managementClient = new(eventBusConfig.EventBusConnectionString);
        _topicClient = CreateTopicClient();
        _logger =
            serviceProvider.GetService(typeof(ILogger<EventBusServiceBus>)) as ILogger<EventBusServiceBus>
            ?? throw new ArgumentException(nameof(_logger));
    }

    private ITopicClient CreateTopicClient()
    {
        if (_topicClient == null || _topicClient.IsClosedOrClosing)
            _topicClient = new TopicClient(
                EventBusConfig.EventBusConnectionString,
                EventBusConfig.DefaultTopicName,
                RetryPolicy.Default
            );

        if (_managementClient.TopicExistsAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult())
            _managementClient.CreateTopicAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult();

        return _topicClient;
    }

    public override void Publish(IntegrationEvent @event)
    {
        string eventName = @event.GetType().Name;

        eventName = ProcessEventName(eventName);

        string eventStr = JsonConvert.SerializeObject(@event);
        byte[] bodyArr = Encoding.UTF8.GetBytes(eventStr);

        Message message =
            new()
            {
                MessageId = Guid.NewGuid().ToString(),
                Label = eventName,
                Body = bodyArr,
            };

        _topicClient.SendAsync(message).GetAwaiter().GetResult();
    }

    public override void Subscribe<T, TH>()
    {
        string eventName = typeof(T).Name;

        eventName = ProcessEventName(eventName);

        if (EventBusSubscriptionManager.HasSubscriptionsForEvent(eventName))
        {
            SubscriptionClient subscriptionClient = CreateSubscriptionClientIfNotExists(eventName);
            RegisterSubscriptionClientMessageHandler(subscriptionClient);
        }

        _logger.LogInformation("Subscribing to event {EventName} with {EventHandler}", eventName, typeof(TH).Name);

        EventBusSubscriptionManager.AddSubscription<T, TH>();
    }

    private void RegisterSubscriptionClientMessageHandler(SubscriptionClient subscriptionClient)
    {
        subscriptionClient.RegisterMessageHandler(
            async (message, token) =>
            {
                string eventName = $"{message.Label}";
                string messageData = Encoding.UTF8.GetString(message.Body);

                if (await ProcessEvent(ProcessEventName(eventName), messageData))
                    await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
            },
            new MessageHandlerOptions(ExceptionReceivedHandler) { MaxConcurrentCalls = 10, AutoComplete = false }
        );
    }

    private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
    {
        Exception exception = exceptionReceivedEventArgs.Exception;
        ExceptionReceivedContext context = exceptionReceivedEventArgs.ExceptionReceivedContext;

        _logger.LogError(
            exception,
            "ERROR handling message: {ExceptionMessage} - Context: {@ExceptionContext}",
            exception.Message,
            context
        );

        return Task.CompletedTask;
    }

    private SubscriptionClient CreateSubscriptionClientIfNotExists(string eventName)
    {
        SubscriptionClient subscriptionClient = CreateSubscriptionClient(eventName);

        bool exists = _managementClient
            .SubscriptionExistsAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName))
            .GetAwaiter()
            .GetResult();

        if (!exists)
        {
            _managementClient
                .CreateSubscriptionAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName))
                .GetAwaiter()
                .GetResult();

            RemoveDefaultRule(subscriptionClient);
        }

        CreateRuleIfNotExists(eventName, subscriptionClient);

        return subscriptionClient;
    }

    private void CreateRuleIfNotExists(string eventName, SubscriptionClient subscriptionClient)
    {
        bool ruleExists;

        try
        {
            RuleDescription rule = _managementClient
                .GetRuleAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName), eventName)
                .GetAwaiter()
                .GetResult();

            ruleExists = rule is not null;
        }
        catch (MessagingEntityNotFoundException)
        {
            ruleExists = false;
        }

        if (!ruleExists)
            subscriptionClient
                .AddRuleAsync(
                    new()
                    {
                        Filter = new CorrelationFilter { Label = eventName },
                        Name = eventName,
                    }
                )
                .GetAwaiter()
                .GetResult();
    }

    private void RemoveDefaultRule(SubscriptionClient subscriptionClient)
    {
        try
        {
            subscriptionClient.RemoveRuleAsync(RuleDescription.DefaultRuleName).GetAwaiter().GetResult();
        }
        catch (MessagingEntityNotFoundException)
        {
            _logger.LogWarning(
                "The messaging entity {DefaultRuleName} could not be found.",
                RuleDescription.DefaultRuleName
            );
        }
    }

    private SubscriptionClient CreateSubscriptionClient(string eventName) =>
        new(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName, GetSubName(eventName));

    public override void Unsubscribe<T, TH>()
    {
        string eventName = typeof(T).Name;

        try
        {
            SubscriptionClient subscriptionClient = CreateSubscriptionClient(eventName);
            subscriptionClient.RemoveRuleAsync(eventName).GetAwaiter().GetResult();
        }
        catch (MessagingEntityNotFoundException)
        {
            _logger.LogWarning("The messaging entity {eventName} could not be found.", eventName);
        }

        _logger.LogInformation("Unsubscribing from event {EventName}", eventName);

        EventBusSubscriptionManager.RemoveSubscription<T, TH>();
    }

    public override void Dispose()
    {
        base.Dispose();

        _managementClient.CloseAsync().GetAwaiter().GetResult();
        _topicClient.CloseAsync().GetAwaiter().GetResult();

        _managementClient = null!;
        _topicClient = null!;
    }
}
