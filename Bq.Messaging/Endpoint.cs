using System;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public interface IMessageSession
    {
        Task Send(object message, SendOptions options);
        Task Send<T>(Action<T> initialize, SendOptions options);
        Task Publish(object message, PublishOptions options);
        Task Publish<T>(Action<T> initialize, PublishOptions options);
        Task Subscribe(Type eventType, SubscribeOptions options);
        Task Unsubscribe(Type eventType, UnsubscribeOptions options);
    }

    public sealed class SubscribeOptions : ExtendableOptions { }
    public sealed class UnsubscribeOptions : ExtendableOptions { }

    public static partial class IMessageSessionExtensions
    {
        public static Task Subscribe<T>(this IMessageSession session) =>
            session.Subscribe(typeof(T), new SubscribeOptions());

        public static Task Unsubscribe<T>(this IMessageSession session) =>
            session.Unsubscribe(typeof(T), new UnsubscribeOptions());

        public static Task Subscribe(this IMessageSession session, Type messageType) =>
            session.Subscribe(messageType, new SubscribeOptions());

        public static Task Unsubscribe(this IMessageSession session, Type messageType) =>
            session.Unsubscribe(messageType, new UnsubscribeOptions());
    }

    public static class SubscriptionExtensions
    {
        public static Task Subscribe<T>(IMessageSession session) => IMessageSessionExtensions.Subscribe<T>(session);
        public static Task Unsubscribe<T>(IMessageSession session) => IMessageSessionExtensions.Unsubscribe<T>(session);
        public static Task Subscribe(IMessageSession session, Type eventType) => IMessageSessionExtensions.Subscribe(session, eventType);
        public static Task Unsubscribe(IMessageSession session, Type eventType) => IMessageSessionExtensions.Unsubscribe(session, eventType);

        public static Task Subscribe<T>(this IMessageSession session, SubscribeOptions options) =>
            session.Subscribe(typeof(T), options ?? new SubscribeOptions());

        public static Task Unsubscribe<T>(this IMessageSession session, UnsubscribeOptions options) =>
            session.Unsubscribe(typeof(T), options ?? new UnsubscribeOptions());
    }

    public interface IEndpointInstance : IMessageSession, IDisposable
    {
        Task Send<T>(T message);
        Task Send<T>(Action<T> initialize);
        Task Send<T>(T message, SendOptions options);
        new Task Send(object message, SendOptions options);
        new Task Send<T>(Action<T> initialize, SendOptions options);
        Task Send(string message, SendOptions options);
        Task Publish<T>(T message);
        Task Publish<T>(Action<T> initialize);
        Task Publish<T>(T message, PublishOptions options);
        new Task Publish(object message, PublishOptions options);
        new Task Publish<T>(Action<T> initialize, PublishOptions options);
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

        public static Task<IEndpointInstance> Start(EndpointConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            // Like NServiceBus, the endpoint entry points discover handlers and sagas. RabbitEndpoint.Start stays explicit.
            if (!configuration.IsSendOnly) configuration.ApplyAssemblyScanning();
            return Task.FromResult<IEndpointInstance>(RabbitEndpoint.Start(configuration));
        }

        sealed class Startable : IStartableEndpoint
        {
            readonly EndpointConfiguration configuration;
            public Startable(EndpointConfiguration configuration) { this.configuration = configuration; }
            public Task<IEndpointInstance> Start() => Endpoint.Start(configuration);
        }
    }
}
