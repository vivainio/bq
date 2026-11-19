using System;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public interface IMessageSession
    {
        Task Send(object message, SendOptions options);
        Task Send<T>(Action<T> initialize, SendOptions options) where T : class, new();
        Task Publish(object message, PublishOptions options);
        Task Publish<T>(Action<T> initialize, PublishOptions options) where T : class, new();
    }

    public interface IEndpointInstance : IMessageSession, IDisposable
    {
        Task Send<T>(T message);
        Task Send<T>(T message, SendOptions options);
        Task Send(string message, SendOptions options);
        Task Publish<T>(T message);
        Task Publish<T>(T message, PublishOptions options);
        Task Stop();
    }

    public interface IStartableEndpoint
    {
        Task<IEndpointInstance> Start();
    }

    public static class Endpoint
    {
        public static Task<IStartableEndpoint> Create(EndpointConfiguration configuration) =>
            Task.FromResult<IStartableEndpoint>(new Startable(configuration ?? throw new ArgumentNullException(nameof(configuration))));

        public static Task<IEndpointInstance> Start(EndpointConfiguration configuration) =>
            Task.FromResult<IEndpointInstance>(RabbitEndpoint.Start(configuration));

        sealed class Startable : IStartableEndpoint
        {
            readonly EndpointConfiguration configuration;
            public Startable(EndpointConfiguration configuration) { this.configuration = configuration; }
            public Task<IEndpointInstance> Start() => Endpoint.Start(configuration);
        }
    }
}
