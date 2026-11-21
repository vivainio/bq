using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Bq.Messaging.ObjectBuilder;

namespace Bq.Messaging
{
    /// <summary>The outermost stage of a delivery attempt, around the physical and logical stages.</summary>
    public interface ITransportReceiveContext : IBehaviorContext
    {
        IncomingMessage Message { get; }

        /// <summary>
        /// Rolls back the attempt's database transaction and returns the message to the queue without
        /// counting a failure. Avoid unconditional aborts, which redeliver the message in a tight loop.
        /// </summary>
        void AbortReceiveOperation();
    }

    internal sealed class AbortReceiveException : Exception
    {
        public AbortReceiveException() : base("The receive operation was aborted.") { }
    }

    internal sealed class TransportReceiveContext : ITransportReceiveContext, IFailureTracking
    {
        readonly RabbitEndpoint.HandlerContext handlerContext;

        public TransportReceiveContext(RabbitEndpoint.HandlerContext handlerContext, IncomingMessage message)
        {
            this.handlerContext = handlerContext;
            Message = message;
        }

        public IncomingMessage Message { get; }
        public ContextBag Extensions => handlerContext.Extensions;
        public IBuilder Builder => handlerContext.Builder;
        internal bool Aborted { get; private set; }
        public ExceptionDispatchInfo DownstreamFailure { get; set; }
        public void AbortReceiveOperation() => Aborted = true;
    }

    public sealed partial class IncomingPipeline
    {
        readonly List<Func<TransportReceiveContext, Func<Task>, Task>> receiveBehaviors = new List<Func<TransportReceiveContext, Func<Task>, Task>>();

        internal void UseReceive(Func<TransportReceiveContext, Func<Task>, Task> behavior) => receiveBehaviors.Add(behavior);

        internal void ReplaceReceive(Func<TransportReceiveContext, Func<Task>, Task> current, Func<TransportReceiveContext, Func<Task>, Task> replacement) =>
            receiveBehaviors[receiveBehaviors.IndexOf(current)] = replacement;

        internal void RemoveReceive(Func<TransportReceiveContext, Func<Task>, Task> behavior) => receiveBehaviors.Remove(behavior);

        internal bool HasReceiveBehaviors => receiveBehaviors.Count != 0;

        internal Func<TransportReceiveContext, Func<Task>, Task> BuildReceive(PipelineSettings settings) =>
            BuildChain(settings.OrderReceive(receiveBehaviors));
    }
}
