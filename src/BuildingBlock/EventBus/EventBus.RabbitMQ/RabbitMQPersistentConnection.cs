using System.Net.Sockets;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace EventBus.RabbitMQ;

public class RabbitMQPersistentConnection(IConnectionFactory connectionFactory, int retryCount = 5) : IDisposable
{
    private readonly IConnectionFactory _connectionFactory = connectionFactory;
    private readonly int _retryCount = retryCount;
    private IConnection _connection = null!;
    private readonly object _lock_object = new object();
    public bool IsConnected => _connection is not null && _connection.IsOpen;
    private bool _disposed;

    public IModel CreateModel() => _connection.CreateModel();

    public void Dispose()
    {
        _connection.Dispose();
        _disposed = true;
    }

    public bool TryConnect()
    {
        lock (_lock_object)
        {
            RetryPolicy policy = Policy
                .Handle<SocketException>()
                .Or<BrokerUnreachableException>()
                .WaitAndRetry(
                    _retryCount,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    (ex, time) => { }
                );

            policy.Execute(() => _connection = _connectionFactory.CreateConnection());

            if (IsConnected)
            {
                _connection.ConnectionShutdown += Connection_ConnectionShutdown;
                _connection.ConnectionBlocked += Connection_ConnectionBlocked;

                return true;
            }

            return false;
        }
    }

    private void Connection_ConnectionBlocked(object? sender, ConnectionBlockedEventArgs e)
    {
        if (_disposed)
            return;

        TryConnect();
    }

    private void Connection_ConnectionShutdown(object? sender, ShutdownEventArgs e)
    {
        if (_disposed)
            return;

        TryConnect();
    }
}
