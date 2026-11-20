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
            if (!container.Kernel.HasComponent(typeof(IMessageMapper)) &&
                !container.Kernel.HasComponent(typeof(IMessageCreator)))
            {
                container.Register(Component.For<IMessageMapper>().Forward<IMessageCreator>()
                    .ImplementedBy<MessageMapper>().LifestyleSingleton());
            }
            container.Register(
                Component.For<IMessageHandlerContext>().Forward<IMessageProcessingContext>().Forward<IPipelineContext>()
                    .UsingFactoryMethod(() => Current).LifestyleScoped(),
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
            new ContainerHandler<TMessage>(container, container.Resolve<IHandleMessages<TMessage>>());

        public void RegisterBehavior<TBehavior>(EndpointConfiguration configuration, string stepId, string description)
            where TBehavior : IBehavior<IIncomingLogicalMessageContext, IIncomingLogicalMessageContext>
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.Pipeline.Register(stepId, new ContainerBehavior<TBehavior>(container), description);
        }

        public void ReplaceBehavior<TBehavior>(EndpointConfiguration configuration, string stepId, string description = null)
            where TBehavior : IBehavior<IIncomingLogicalMessageContext, IIncomingLogicalMessageContext>
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.Pipeline.Replace(stepId, new ContainerBehavior<TBehavior>(container), description);
        }

        sealed class ContainerBehavior<TBehavior> : IBehavior<IIncomingLogicalMessageContext, IIncomingLogicalMessageContext>
            where TBehavior : IBehavior<IIncomingLogicalMessageContext, IIncomingLogicalMessageContext>
        {
            readonly IWindsorContainer container;

            public ContainerBehavior(IWindsorContainer container) { this.container = container; }

            public async Task Invoke(IIncomingLogicalMessageContext context, Func<IIncomingLogicalMessageContext, Task> next)
            {
                var behavior = container.Resolve<TBehavior>();
                try { await behavior.Invoke(context, next).ConfigureAwait(false); }
                finally { container.Release(behavior); }
            }
        }

        public void HandleSagaNotFound(EndpointConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.RegisterSagaNotFoundHandler(_ =>
                new ContainerSagaNotFoundHandler(container, container.Resolve<IHandleSagaNotFound>()));
        }

        public void RegisterSaga<TSaga, TData>(EndpointConfiguration configuration)
            where TSaga : Saga<TData>, new() where TData : class, new() =>
            RegisterSaga<TSaga, TData>(configuration, () => new TSaga());

        public void RegisterSaga<TSaga, TData>(EndpointConfiguration configuration, Func<TSaga> definitionFactory)
            where TSaga : Saga<TData> where TData : class, new()
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.RegisterSaga<TSaga, TData>(() => container.Resolve<TSaga>(), definitionFactory, saga => container.Release(saga));
        }

        public void RegisterSaga<TSaga, TData>(EndpointConfiguration configuration, Action<SagaMappings<TSaga, TData>> configure)
            where TSaga : Saga<TData> where TData : class, new()
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.RegisterSaga<TSaga, TData>(() => container.Resolve<TSaga>(), configure, saga => container.Release(saga));
        }

        sealed class ContainerSagaNotFoundHandler : IHandleSagaNotFound, IDisposable
        {
            readonly IHandleSagaNotFound handler;

            readonly IWindsorContainer container;

            public ContainerSagaNotFoundHandler(IWindsorContainer container, IHandleSagaNotFound handler)
            {
                this.container = container;
                this.handler = handler;
            }

            public void Dispose() => container.Release(handler);

            public Task Handle(object message, IMessageProcessingContext context) => handler.Handle(message, context);
        }

        sealed class ContainerHandler<TMessage> : IHandleMessages<TMessage>, IDisposable
        {
            readonly IHandleMessages<TMessage> handler;

            readonly IWindsorContainer container;

            public ContainerHandler(IWindsorContainer container, IHandleMessages<TMessage> handler)
            {
                this.container = container;
                this.handler = handler;
            }

            public void Dispose() => container.Release(handler);

            public Task Handle(TMessage message, IMessageHandlerContext context) => handler.Handle(message, context);
        }
    }
}
