using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging;
using Castle.MicroKernel.Lifestyle;
using Castle.MicroKernel.Registration;
using Castle.Windsor;

namespace Bq.Extensions.CastleWindsor
{
    public sealed class WindsorMessaging
    {
        readonly IWindsorContainer container;
        readonly AsyncLocal<IMessageHandlerContext> active = new AsyncLocal<IMessageHandlerContext>();

        public WindsorMessaging(IWindsorContainer container, Func<IUniformSession> outsideSession = null)
        {
            this.container = container ?? throw new ArgumentNullException(nameof(container));
            container.Register(
                Component.For<IMessageHandlerContext>().UsingFactoryMethod(() => Current).LifestyleScoped(),
                Component.For<SynchronizedStorageSession>().UsingFactoryMethod(() =>
                    new StorageSessionView(Current.SynchronizedStorageSession ??
                        throw new InvalidOperationException("No active synchronized storage session.")))
                    .LifestyleScoped(),
                Component.For<IUniformSession>().UsingFactoryMethod(() =>
                    active.Value?.UniformSession ?? (outsideSession != null
                        ? outsideSession() : throw new InvalidOperationException("No active message scope."))).LifestyleScoped());
        }

        // The endpoint owns the persistence session and commits after the Windsor scope closes.
        sealed class StorageSessionView : SynchronizedStorageSession
        {
            readonly SynchronizedStorageSession session;

            public StorageSessionView(SynchronizedStorageSession session) { this.session = session; }
            public DbConnection Connection => session.Connection;
            public DbTransaction Transaction => session.Transaction;
        }

        public IMessageHandlerContext Current => active.Value ?? throw new InvalidOperationException("No active message scope.");

        public void Attach(EndpointConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.Incoming.Use(async (incoming, next) =>
            {
                var previous = active.Value;
                active.Value = incoming.HandlerContext;
                try
                {
                    using (container.BeginScope())
                        await next().ConfigureAwait(false);
                }
                finally { active.Value = previous; }
            });
        }

        public void Handle<TMessage>(EndpointConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.Handle<TMessage>(ResolveHandler<TMessage>);
        }

        public void Handle<TMessage>(EndpointConfiguration configuration, bool subscribe)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.Handle<TMessage>(ResolveHandler<TMessage>, subscribe);
        }

        IHandleMessages<TMessage> ResolveHandler<TMessage>() =>
            new ContainerHandler<TMessage>(container.Resolve<IHandleMessages<TMessage>>());

        public void HandleSagaNotFound(EndpointConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.RegisterSagaNotFoundHandler(_ =>
                new ContainerSagaNotFoundHandler(container.Resolve<IHandleSagaNotFound>()));
        }

        public void RegisterSaga<TSaga, TData>(EndpointConfiguration configuration, Action<SagaMappings<TSaga, TData>> configure)
            where TSaga : Saga<TData> where TData : class, new()
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.RegisterSaga<TSaga, TData>(() => container.Resolve<TSaga>(), configure, saga => container.Release(saga));
        }

        sealed class ContainerSagaNotFoundHandler : IHandleSagaNotFound
        {
            readonly IHandleSagaNotFound handler;

            public ContainerSagaNotFoundHandler(IHandleSagaNotFound handler) { this.handler = handler; }

            public Task Handle(object message, IMessageProcessingContext context) => handler.Handle(message, context);
        }

        // Windsor releases the resolved handler with its scope.
        sealed class ContainerHandler<TMessage> : IHandleMessages<TMessage>
        {
            readonly IHandleMessages<TMessage> handler;

            public ContainerHandler(IHandleMessages<TMessage> handler) { this.handler = handler; }

            public Task Handle(TMessage message, IMessageHandlerContext context) => handler.Handle(message, context);
        }
    }
}
