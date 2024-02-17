using EventBus.Base.Abstraction;
using EventBus.Base.SubManagers;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;

namespace EventBus.Base.Events;

public class BaseEventBus : IEventBus
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IEventBusSubscriptionManager _eventBusSubscriptionManager;
    private EventBusConfig _eventBusConfig;

    public BaseEventBus(EventBusConfig eventBusConfig, IServiceProvider serviceProvider)
    {
        _eventBusConfig = eventBusConfig;
        _serviceProvider = serviceProvider;
        _eventBusSubscriptionManager = new InMemoryEventBusSubscriptionManager(ProcessEventName);
    }

    public virtual string ProcessEventName(string eventName)
    {
        if (_eventBusConfig.DeleteEventPrefix)
            eventName = eventName.TrimStart([.. _eventBusConfig.EventNamePrefix]);

        if (_eventBusConfig.DeleteEventSuffix)
            eventName = eventName.TrimEnd([.. _eventBusConfig.EventNameSuffix]);

        return eventName;
    }

    public virtual string GetSubName(string eventName) =>
        $"{_eventBusConfig.SubscriberClientAppName}.{ProcessEventName(eventName)}";

    public virtual void Dispose()
    {
        _eventBusConfig = null!;
    }

    public async Task<bool> ProcessEvent(string eventName, string message)
    {
        eventName = ProcessEventName(eventName);

        bool processed = false;

        if (_eventBusSubscriptionManager.HasSubscriptionsForEvent(eventName))
        {
            IEnumerable<SubscriptionInfo> subscriptions = _eventBusSubscriptionManager.GetHandlersForEvent(eventName);

            using AsyncServiceScope scope = _serviceProvider.CreateAsyncScope();

            foreach (SubscriptionInfo subscription in subscriptions)
            {
                object? handler = _serviceProvider.GetService(subscription.HandlerType);
                if (handler is null)
                    continue;

                Type eventType = _eventBusSubscriptionManager.GetEventTypeByName(
                    $"{_eventBusConfig.EventNamePrefix}{eventName}{_eventBusConfig.EventNameSuffix}"
                )!;
                object integrationEvent = JsonConvert.DeserializeObject(message, eventType)!;

                Type concreteType = typeof(IIntegrationEventHandler<>).MakeGenericType(eventType);
                await (Task)concreteType.GetMethod("Handle")?.Invoke(handler, [integrationEvent])!;
            }

            processed = true;
        }

        return processed;
    }

    public void Publish(IntegrationEvent @event)
    {
        throw new NotImplementedException();
    }

    public void Subscribe<T, TH>()
        where T : IntegrationEvent
        where TH : IIntegrationEventHandler<T>
    {
        throw new NotImplementedException();
    }

    public void Unsubscribe<T, TH>()
        where T : IntegrationEvent
        where TH : IIntegrationEventHandler<T>
    {
        throw new NotImplementedException();
    }
}
