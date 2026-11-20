using System;
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
                Component.For<IUniformSession>().UsingFactoryMethod(() =>
                    active.Value?.UniformSession ?? (outsideSession != null
                        ? outsideSession() : throw new InvalidOperationException("No active message scope."))).LifestyleScoped());
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

        public void Handle<TMessage>(EndpointConfiguration configuration, bool subscribe = false)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.Handle<TMessage>(() => new ContainerHandler<TMessage>(container.Resolve<IHandleMessages<TMessage>>()), subscribe);
        }

        public void HandleSagaNotFound(EndpointConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.RegisterSagaNotFoundHandler(_ =>
                new ContainerSagaNotFoundHandler(container.Resolve<IHandleSagaNotFound>()));
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
