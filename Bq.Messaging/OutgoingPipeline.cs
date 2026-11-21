using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging.ObjectBuilder;

namespace Bq.Messaging
{
    public interface IOutgoingContext : IBehaviorContext, IPipelineContext
    {
        string MessageId { get; }
        Dictionary<string, string> Headers { get; }
    }

    public interface IOutgoingSendContext : IOutgoingContext { OutgoingLogicalMessage Message { get; } }
    public interface IOutgoingPublishContext : IOutgoingContext { OutgoingLogicalMessage Message { get; } }
    public interface IOutgoingReplyContext : IOutgoingContext { OutgoingLogicalMessage Message { get; } }

    public interface IOutgoingLogicalMessageContext : IOutgoingContext
    {
        OutgoingLogicalMessage Message { get; }
        IReadOnlyCollection<Routing.RoutingStrategy> RoutingStrategies { get; }
        void UpdateMessage(object newInstance);
    }

    public interface IOutgoingPhysicalMessageContext : IOutgoingContext
    {
        byte[] Body { get; }
        IReadOnlyCollection<Routing.RoutingStrategy> RoutingStrategies { get; }
        void UpdateMessage(byte[] body);
    }

    internal enum OutgoingStage { Send, Publish, Reply, Logical, Physical }

    internal interface IFailureTracking { ExceptionDispatchInfo DownstreamFailure { get; set; } }

    internal sealed class OutgoingContext : IOutgoingSendContext, IOutgoingPublishContext, IOutgoingReplyContext,
        IOutgoingLogicalMessageContext, IOutgoingPhysicalMessageContext, IFailureTracking
    {
        readonly IMessageSession session;
        readonly Func<IBuilder> builder;

        public OutgoingContext(IMessageSession session, Func<IBuilder> builder, string messageId, Type contract, object message,
            Dictionary<string, string> headers, ContextBag extensions, IReadOnlyCollection<Routing.RoutingStrategy> routing)
        {
            this.session = session;
            this.builder = builder;
            MessageId = messageId;
            Contract = contract;
            Instance = message;
            Headers = headers;
            Extensions = extensions;
            RoutingStrategies = routing;
        }

        internal Type Contract { get; }
        internal object Instance { get; private set; }
        internal byte[] CurrentBody { get; set; }
        public string MessageId { get; }
        public Dictionary<string, string> Headers { get; }
        public ContextBag Extensions { get; }
        public IBuilder Builder => builder();
        public IReadOnlyCollection<Routing.RoutingStrategy> RoutingStrategies { get; }
        public OutgoingLogicalMessage Message => new OutgoingLogicalMessage(Contract, Instance);
        public byte[] Body => CurrentBody;
        public ExceptionDispatchInfo DownstreamFailure { get; set; }

        public void UpdateMessage(object newInstance)
        {
            if (newInstance == null) throw new ArgumentNullException(nameof(newInstance));
            if (!Contract.IsInstanceOfType(newInstance))
                throw new ArgumentException("An outgoing replacement must implement the declared message contract.", nameof(newInstance));
            Instance = newInstance;
        }

        public void UpdateMessage(byte[] body) => CurrentBody = body ?? throw new ArgumentNullException(nameof(body));

        public Task Send(object message, SendOptions options) => session.Send(message, options);
        public Task Send<T>(Action<T> initialize, SendOptions options) => session.Send(initialize, options);
        public Task Publish(object message, PublishOptions options) => session.Publish(message, options);
        public Task Publish<T>(Action<T> initialize, PublishOptions options) => session.Publish(initialize, options);
    }
}

namespace Bq.Messaging.Routing
{
    public abstract class AddressTag { }

    public sealed class UnicastAddressTag : AddressTag
    {
        public UnicastAddressTag(string destination) { Destination = destination; }
        public string Destination { get; }
    }

    public sealed class MulticastAddressTag : AddressTag
    {
        public MulticastAddressTag(Type messageType) { MessageType = messageType; }
        public Type MessageType { get; }
    }

    public abstract class RoutingStrategy
    {
        public abstract AddressTag Apply(Dictionary<string, string> headers);
    }

    public class UnicastRoutingStrategy : RoutingStrategy
    {
        readonly string destination;
        public UnicastRoutingStrategy(string destination) { this.destination = destination; }
        public override AddressTag Apply(Dictionary<string, string> headers) => new UnicastAddressTag(destination);
    }

    public class MulticastRoutingStrategy : RoutingStrategy
    {
        readonly Type eventType;
        public MulticastRoutingStrategy(Type eventType) { this.eventType = eventType; }
        public override AddressTag Apply(Dictionary<string, string> headers) => new MulticastAddressTag(eventType);
    }
}
