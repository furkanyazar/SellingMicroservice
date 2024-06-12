using EventBus.Base;
using EventBus.Base.Abstraction;
using EventBus.Factory;
using EventBus.UnitTest.Events.EventHandlers;
using EventBus.UnitTest.Events.Events;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EventBus.UnitTest
{
    [TestClass]
    public class EventBusUnitTests
    {
        private readonly ServiceCollection _services;

        public EventBusUnitTests()
        {
            _services = new ServiceCollection();
            _services.AddLogging(conf => conf.AddConsole());
        }

        [TestMethod]
        public void subscribe_event_on_rabbitmq_test()
        {
            _services.AddSingleton<IEventBus>(sp => EventBusFactory.Create(GetRabbitMQConfig(), sp));

            ServiceProvider serviceProvider = _services.BuildServiceProvider();

            IEventBus eventBus = serviceProvider.GetRequiredService<IEventBus>();

            eventBus.Subscribe<OrderCreatedIntegrationEvent, OrderCreatedIntegrationEventHandler>();
        }

        [TestMethod]
        public void send_message_to_rabbitmq_test()
        {
            _services.AddSingleton<IEventBus>(sp => EventBusFactory.Create(GetRabbitMQConfig(), sp));

            ServiceProvider serviceProvider = _services.BuildServiceProvider();

            IEventBus eventBus = serviceProvider.GetRequiredService<IEventBus>();

            eventBus.Publish(new OrderCreatedIntegrationEvent(1));
        }

        private EventBusConfig GetRabbitMQConfig()
        {
            return new()
            {
                ConnectionRetryCount = 5,
                SubscriberClientAppName = "EventBus.UnitTest",
                DefaultTopicName = "SellingMicroserviceTopicName",
                EventBusType = EventBusType.RabbitMQ,
                EventNameSuffix = "IntegrationEvent",
            };
        }
    }
}
