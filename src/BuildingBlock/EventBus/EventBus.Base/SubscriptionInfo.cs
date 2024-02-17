namespace EventBus.Base;

public class SubscriptionInfo(Type handlerType)
{
    public Type HandlerType { get; } = handlerType ?? throw new ArgumentNullException(nameof(handlerType));

    public static SubscriptionInfo Typed(Type handlerType) => new(handlerType);
}
