using System;
using System.Linq;
using System.Threading.Tasks;
using Bq.Messaging.ObjectBuilder;

namespace Bq.Messaging
{
    /// <summary>Resolves a behavior from the builder of the message attempt each time it runs, for NServiceBus-style builder factories.</summary>
    internal sealed class LazyBehavior<TContext> : IBehavior<TContext, TContext> where TContext : IBehaviorContext
    {
        readonly Func<IBuilder, object> factory;

        public LazyBehavior(Func<IBuilder, object> factory) { this.factory = factory; }

        public Task Invoke(TContext context, Func<TContext, Task> next)
        {
            var behavior = (IBehavior<TContext, TContext>)(factory(context.Builder) ?? throw new InvalidOperationException("The behavior factory returned null."));
            return behavior.Invoke(context, next);
        }
    }

    internal static class LazyBehaviors
    {
        static readonly Type[] ContextTypes =
        {
            typeof(IIncomingLogicalMessageContext), typeof(IIncomingPhysicalMessageContext), typeof(IInvokeHandlerContext),
            typeof(ITransportReceiveContext), typeof(IDeliveryContext), typeof(IOutgoingSendContext), typeof(IOutgoingPublishContext),
            typeof(IOutgoingReplyContext), typeof(IOutgoingLogicalMessageContext), typeof(IOutgoingPhysicalMessageContext)
        };

        public static IBehavior For(Type behaviorType, Func<IBuilder, object> factory)
        {
            foreach (var context in ContextTypes)
                if (typeof(IBehavior<,>).MakeGenericType(context, context).IsAssignableFrom(behaviorType))
                    return (IBehavior)Activator.CreateInstance(typeof(LazyBehavior<>).MakeGenericType(context), factory);
            throw new NotSupportedException(behaviorType.FullName + " does not implement a supported pipeline behavior stage.");
        }
    }
}
