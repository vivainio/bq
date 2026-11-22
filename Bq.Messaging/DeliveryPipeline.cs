using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Bq.Messaging.ObjectBuilder;

namespace Bq.Messaging
{
    /// <summary>
    /// The outermost stage, around everything an endpoint does with a delivery, including opening the persistence session and claiming the
    /// message. Because it runs before the session opens, a behavior here can set up ambient state, such as the tenant a database connection
    /// is chosen by, and have it flow to the session, the handlers and the failure handling through <c>await next()</c>.
    /// </summary>
    /// <remarks>
    /// There is no session at this stage. A behavior that throws before calling next rejects the delivery: it goes to the error queue without
    /// touching the database, with the exception recorded in its headers. A behavior that returns without calling next discards the message.
    /// </remarks>
    public interface IDeliveryContext : IBehaviorContext
    {
        string MessageId { get; }
        /// <summary>The AMQP type of the message, which is its contract name.</summary>
        string MessageType { get; }
        /// <summary>The delivery's headers as text. Changes to this copy are not passed on.</summary>
        IReadOnlyDictionary<string, string> Headers { get; }
    }

    internal sealed class DeliveryContext : IDeliveryContext, IFailureTracking
    {
        public DeliveryContext(IBuilder builder, string messageId, string messageType, Dictionary<string, string> headers)
        {
            Builder = builder;
            MessageId = messageId;
            MessageType = messageType;
            Headers = headers;
        }

        public string MessageId { get; }
        public string MessageType { get; }
        public IReadOnlyDictionary<string, string> Headers { get; }
        public ContextBag Extensions { get; } = new ContextBag();
        public IBuilder Builder { get; }
        public ExceptionDispatchInfo DownstreamFailure { get; set; }
    }

    public sealed partial class IncomingPipeline
    {
        readonly List<Func<DeliveryContext, Func<Task>, Task>> deliveryBehaviors = new List<Func<DeliveryContext, Func<Task>, Task>>();

        internal void UseDelivery(Func<DeliveryContext, Func<Task>, Task> behavior) => deliveryBehaviors.Add(behavior);

        internal void ReplaceDelivery(Func<DeliveryContext, Func<Task>, Task> current, Func<DeliveryContext, Func<Task>, Task> replacement) =>
            deliveryBehaviors[deliveryBehaviors.IndexOf(current)] = replacement;

        internal void RemoveDelivery(Func<DeliveryContext, Func<Task>, Task> behavior) => deliveryBehaviors.Remove(behavior);

        internal bool HasDeliveryBehaviors => deliveryBehaviors.Count != 0;

        internal Func<DeliveryContext, Func<Task>, Task> BuildDelivery(PipelineSettings settings) =>
            BuildChain(settings.OrderDelivery(deliveryBehaviors));
    }
}
