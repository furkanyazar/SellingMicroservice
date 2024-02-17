using EventBus.Base;
using EventBus.Base.Abstraction;

namespace EventBus.Factory;

public static class EventBusFactory
{
    public static IEventBus Create(EventBusConfig eventBusConfig, IServiceProvider serviceProvider) =>
        eventBusConfig.EventBusType switch
        {
            EventBusType.AzureServiceBus => new AzureServiceBus.EventBusServiceBus(eventBusConfig, serviceProvider),
            _ => new RabbitMQ.EventBusServiceBus(eventBusConfig, serviceProvider),
        };
}
