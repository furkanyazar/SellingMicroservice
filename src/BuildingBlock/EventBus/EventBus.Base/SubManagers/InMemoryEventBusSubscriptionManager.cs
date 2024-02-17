using EventBus.Base.Abstraction;
using EventBus.Base.Events;

namespace EventBus.Base.SubManagers;

public class InMemoryEventBusSubscriptionManager(Func<string, string> eventNameGetter) : IEventBusSubscriptionManager
{
    private readonly Dictionary<string, List<SubscriptionInfo>> _handlers = [];
    private readonly List<Type> _eventTypes = [];

    public event EventHandler<string> OnEventRemoved = null!;
    public Func<string, string> EventNameGetter = eventNameGetter;

    public bool IsEmpty => _handlers.Keys.Count == 0;

    public void Clear() => _handlers.Clear();

    public void AddSubscription<T, TH>()
        where T : IntegrationEvent
        where TH : IIntegrationEventHandler<T>
    {
        string eventName = GetEventKey<T>();

        AddSubscription(typeof(TH), eventName);

        if (!_eventTypes.Contains(typeof(T)))
            _eventTypes.Add(typeof(T));
    }

    private void AddSubscription(Type handlerType, string eventName)
    {
        if (!HasSubscriptionsForEvent(eventName))
            _handlers.Add(eventName, []);

        if (_handlers[eventName].Any(c => c.HandlerType == handlerType))
            throw new ArgumentException(
                $"Handler type {handlerType.Name} already registered for '{eventName}'",
                nameof(handlerType)
            );

        _handlers[eventName].Add(SubscriptionInfo.Typed(handlerType));
    }

    public void RemoveSubscription<T, TH>()
        where T : IntegrationEvent
        where TH : IIntegrationEventHandler<T>
    {
        SubscriptionInfo? handlerToRemove = FindSubscriptionToRemove<T, TH>();
        string eventName = GetEventKey<T>();
        RemoveHandler(eventName, handlerToRemove);
    }

    private void RemoveHandler(string eventName, SubscriptionInfo? subsToRemove)
    {
        if (subsToRemove is not null)
        {
            _handlers[eventName].Remove(subsToRemove);

            if (_handlers[eventName].Count == 0)
            {
                _handlers.Remove(eventName);

                Type? eventType = _eventTypes.SingleOrDefault(c => c.Name == eventName);
                if (eventType is not null)
                    _eventTypes.Remove(eventType);

                RaiseOnEventRemoved(eventName);
            }
        }
    }

    public IEnumerable<SubscriptionInfo> GetHandlersForEvent<T>()
        where T : IntegrationEvent
    {
        string key = GetEventKey<T>();
        return GetHandlersForEvent(key);
    }

    public IEnumerable<SubscriptionInfo> GetHandlersForEvent(string eventName) => _handlers[eventName];

    private void RaiseOnEventRemoved(string eventName)
    {
        EventHandler<string> handler = OnEventRemoved;
        handler?.Invoke(this, eventName);
    }

    private SubscriptionInfo? FindSubscriptionToRemove<T, TH>()
        where T : IntegrationEvent
        where TH : IIntegrationEventHandler<T>
    {
        string eventName = GetEventKey<T>();
        return FindSubscriptionToRemove(eventName, typeof(TH));
    }

    private SubscriptionInfo? FindSubscriptionToRemove(string eventName, Type handlerType)
    {
        if (!HasSubscriptionsForEvent(eventName))
            return null;

        return _handlers[eventName].SingleOrDefault(c => c.HandlerType == handlerType);
    }

    public bool HasSubscriptionsForEvent<T>()
        where T : IntegrationEvent
    {
        string key = GetEventKey<T>();
        return HasSubscriptionsForEvent(key);
    }

    public bool HasSubscriptionsForEvent(string eventName) => _handlers.ContainsKey(eventName);

    public Type? GetEventTypeByName(string eventName) => _eventTypes.SingleOrDefault(c => c.Name == eventName);

    public string GetEventKey<T>()
    {
        string eventName = typeof(T).Name;
        return EventNameGetter(eventName);
    }
}
