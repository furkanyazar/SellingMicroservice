using EventBus.Base.Abstraction;
using EventBus.Base.SubManagers;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;

namespace EventBus.Base.Events;

public abstract class BaseEventBus : IEventBus
{
    public readonly IServiceProvider ServiceProvider;
    public readonly IEventBusSubscriptionManager EventBusSubscriptionManager;
    public EventBusConfig EventBusConfig { get; set; }

    public BaseEventBus(EventBusConfig eventBusConfig, IServiceProvider serviceProvider)
    {
        EventBusConfig = eventBusConfig;
        ServiceProvider = serviceProvider;
        EventBusSubscriptionManager = new InMemoryEventBusSubscriptionManager(ProcessEventName);
    }

    public virtual string ProcessEventName(string eventName)
    {
        if (EventBusConfig.DeleteEventPrefix)
            eventName = eventName.TrimStart([.. EventBusConfig.EventNamePrefix]);

        if (EventBusConfig.DeleteEventSuffix)
            eventName = eventName.TrimEnd([.. EventBusConfig.EventNameSuffix]);

        return eventName;
    }

    public virtual string GetSubName(string eventName) =>
        $"{EventBusConfig.SubscriberClientAppName}.{ProcessEventName(eventName)}";

    public virtual void Dispose()
    {
        EventBusConfig = null!;
        EventBusSubscriptionManager.Clear();
    }

    public async Task<bool> ProcessEvent(string eventName, string message)
    {
        eventName = ProcessEventName(eventName);

        bool processed = false;

        if (EventBusSubscriptionManager.HasSubscriptionsForEvent(eventName))
        {
            IEnumerable<SubscriptionInfo> subscriptions = EventBusSubscriptionManager.GetHandlersForEvent(eventName);

            using AsyncServiceScope scope = ServiceProvider.CreateAsyncScope();

            foreach (SubscriptionInfo subscription in subscriptions)
            {
                object? handler = ServiceProvider.GetService(subscription.HandlerType);
                if (handler is null)
                    continue;

                Type eventType = EventBusSubscriptionManager.GetEventTypeByName(
                    $"{EventBusConfig.EventNamePrefix}{eventName}{EventBusConfig.EventNameSuffix}"
                )!;
                object integrationEvent = JsonConvert.DeserializeObject(message, eventType)!;

                Type concreteType = typeof(IIntegrationEventHandler<>).MakeGenericType(eventType);
                await (Task)concreteType.GetMethod("Handle")?.Invoke(handler, [integrationEvent])!;
            }

            processed = true;
        }

        return processed;
    }

    public abstract void Publish(IntegrationEvent @event);

    public abstract void Subscribe<T, TH>()
        where T : IntegrationEvent
        where TH : IIntegrationEventHandler<T>;

    public abstract void Unsubscribe<T, TH>()
        where T : IntegrationEvent
        where TH : IIntegrationEventHandler<T>;
}
