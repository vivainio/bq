using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public abstract class MessageOptions
    {
        internal const string WireIdHeader = "x-bq-forwarded-message-id";
        internal const string ReplyAddressHeader = "x-bq-override-reply-to";
        internal bool ImmediateDispatch { get; private set; }
        internal string MessageId { get; private set; }
        internal string ReplyAddress { get; private set; }

        public void SetMessageId(string messageId) => MessageId = ValidateAddressField(messageId, nameof(messageId));

        internal void SetReplyAddress(string address) => ReplyAddress = ValidateAddressField(address, nameof(address));

        static string ValidateAddressField(string value, string parameter)
        {
            if (string.IsNullOrWhiteSpace(value) || System.Text.Encoding.UTF8.GetByteCount(value) > 255)
                throw new ArgumentException("A nonempty value of at most 255 UTF-8 bytes is required.", parameter);
            return value;
        }

        internal IDictionary<string, object> ApplyEnvelope(IDictionary<string, object> headers)
        {
            var copy = new Dictionary<string, object>(headers);
            if (MessageId != null) copy[WireIdHeader] = MessageId;
            if (ReplyAddress != null) copy[ReplyAddressHeader] = ReplyAddress;
            return copy;
        }

        public virtual void RequireImmediateDispatch() => ImmediateDispatch = true;

        internal IDictionary<string, object> Headers { get; } = new Dictionary<string, object>();

        public void SetHeader(string name, object value)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Header name is required.", nameof(name));
            if (name.StartsWith("x-bq-", StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("The x-bq- header prefix is reserved.", nameof(name));
            Headers[name] = value;
        }

        internal IUniformSession Apply(IUniformSession session)
        {
            foreach (var header in Headers) session = session.WithHeader(header.Key, header.Value);
            if (MessageId == null && ReplyAddress == null) return session;
            if (session is IEnvelopeSession envelope) return envelope.WithEnvelope(this);
            throw new NotSupportedException("This session does not support setting a message ID or reply address.");
        }
    }

    public sealed class SendOptions : MessageOptions
    {
        TimeSpan? delay;
        DateTimeOffset? deliverAt;
        internal string Destination { get; private set; }
        internal bool Local { get; private set; }

        public void RouteReplyTo(string address) => SetReplyAddress(address);

        public void SetDestination(string destination)
        {
            if (string.IsNullOrWhiteSpace(destination)) throw new ArgumentException("Destination is required.", nameof(destination));
            if (Local) throw new InvalidOperationException("A local destination has already been selected.");
            Destination = destination;
        }

        public void RouteToThisEndpoint()
        {
            if (Destination != null) throw new InvalidOperationException("An explicit destination has already been selected.");
            Local = true;
        }

        public override void RequireImmediateDispatch()
        {
            if (delay.HasValue || deliverAt.HasValue) throw new InvalidOperationException("Immediate dispatch cannot be scheduled.");
            base.RequireImmediateDispatch();
        }

        public void DelayDeliveryWith(TimeSpan value)
        {
            if (ImmediateDispatch) throw new InvalidOperationException("Immediate dispatch cannot be scheduled.");
            if (value < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(value));
            delay = value;
            deliverAt = null;
        }

        public void DoNotDeliverBefore(DateTimeOffset value)
        {
            if (ImmediateDispatch) throw new InvalidOperationException("Immediate dispatch cannot be scheduled.");
            deliverAt = value;
            delay = null;
        }

        internal DateTimeOffset? DueAt => delay.HasValue ? DateTimeOffset.UtcNow.Add(delay.Value) : deliverAt;
        internal TimeSpan? DeliveryDelay => delay;
        internal DateTimeOffset? DeliveryDate => deliverAt;
    }

    public sealed class PublishOptions : MessageOptions { }
    public sealed class ReplyOptions : MessageOptions
    {
        internal string CorrelationId { get; set; }
        internal string Destination { get; private set; }

        public void RouteReplyTo(string address) => SetReplyAddress(address);

        public void SetDestination(string destination)
        {
            if (string.IsNullOrWhiteSpace(destination)) throw new ArgumentException("Destination is required.", nameof(destination));
            Destination = destination;
        }
    }

    internal interface IEnvelopeSession
    {
        IUniformSession WithEnvelope(MessageOptions options);
    }

    internal interface IReplyWithOptions
    {
        Task ReplyWithOptions<T>(T message, ReplyOptions options);
    }

    internal interface IImmediateDispatch
    {
        Task SendImmediately<T>(T message, string destination, bool local);
        Task PublishImmediately<T>(T message);
    }

    public static class MessageOptionsExtensions
    {
        public static string GetMessageId(this MessageOptions options) => options.MessageId;
        public static IReadOnlyDictionary<string, string> GetHeaders(this MessageOptions options) => MessageHeaders.AsText(options.Headers);
        public static string GetDestination(this SendOptions options) => options.Destination;
        public static string GetDestination(this ReplyOptions options) => options.Destination;
        public static string GetReplyToRoute(this SendOptions options) => options.ReplyAddress;
        public static string GetReplyToRoute(this ReplyOptions options) => options.ReplyAddress;
        public static bool IsRoutingToThisEndpoint(this SendOptions options) => options.Local;
        public static TimeSpan? GetDeliveryDelay(this SendOptions options) => options.DeliveryDelay;
        public static DateTimeOffset? GetDeliveryDate(this SendOptions options) => options.DeliveryDate;

        internal static T Initialize<T>(Action<T> initialize)
        {
            if (initialize == null) throw new ArgumentNullException(nameof(initialize));
            var message = (T)InterfaceMessage.Create(typeof(T));
            initialize(message);
            return message;
        }

        public static Task Send(this IPipelineContext context, object message) => context.Send(message, new SendOptions());
        public static Task Publish(this IPipelineContext context, object message) => context.Publish(message, new PublishOptions());
        public static Task Reply(this IMessageProcessingContext context, object message) => context.Reply(message, new ReplyOptions());

        public static Task Send<T>(this IPipelineContext context, Action<T> initialize) =>
            context.Send(initialize, new SendOptions());

        public static Task Publish<T>(this IPipelineContext context, Action<T> initialize) =>
            context.Publish(initialize, new PublishOptions());

        public static Task Reply<T>(this IMessageProcessingContext context, Action<T> initialize) =>
            context.Reply(initialize, new ReplyOptions());

        public static Task Send<T>(this IMessageSession session, Action<T> initialize) =>
            session.Send(initialize, new SendOptions());

        public static Task Publish<T>(this IMessageSession session, Action<T> initialize) =>
            session.Publish(initialize, new PublishOptions());

        public static Task Send<T>(this IMessageHandlerContext context, Action<T> initialize) =>
            context.Send(initialize, new SendOptions());

        public static Task Publish<T>(this IMessageHandlerContext context, Action<T> initialize) =>
            context.Publish(initialize, new PublishOptions());

        public static Task Reply<T>(this IMessageHandlerContext context, Action<T> initialize) =>
            context.Reply(initialize, new ReplyOptions());

        public static Task Send<T>(this IUniformSession session, Action<T> initialize) =>
            session.Send(initialize, new SendOptions());

        public static Task Publish<T>(this IUniformSession session, Action<T> initialize) =>
            session.Publish(initialize, new PublishOptions());

        public static Task SendWithOptions<T>(this IUniformSession session, T message, SendOptions options) =>
            session.Send(message, options);

        internal static Task SendUsingOptions<T>(IUniformSession session, T message, SendOptions options,
            string destination = null, bool local = false)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if ((destination != null || local) && (options.Destination != null || options.Local))
                throw new ArgumentException("A destination has already been supplied.", nameof(options));
            destination = destination ?? options.Destination;
            local = local || options.Local;
            var dueAt = options.DueAt;
            session = options.Apply(session);
            if (options.ImmediateDispatch)
            {
                if (session is IImmediateDispatch immediate) return immediate.SendImmediately(message, destination, local);
                throw new NotSupportedException("This session does not support immediate dispatch.");
            }
            if (local) return dueAt.HasValue ? session.SendLocalAt(message, dueAt.Value) : session.SendLocal(message);
            if (destination != null) return dueAt.HasValue ? session.SendAt(destination, message, dueAt.Value) : session.Send(destination, message);
            return dueAt.HasValue ? session.SendAt(message, dueAt.Value) : session.Send(message);
        }

        public static Task Send<T>(this IUniformSession session, string destination, T message, SendOptions options) =>
            SendUsingOptions(session, message, destination: destination ?? throw new ArgumentNullException(nameof(destination)), options: options);

        public static Task SendLocal<T>(this IUniformSession session, T message, SendOptions options) =>
            SendUsingOptions(session, message, options, local: true);

        public static Task Publish<T>(this IUniformSession session, T message, PublishOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            session = options.Apply(session);
            if (!options.ImmediateDispatch) return session.Publish(message);
            if (session is IImmediateDispatch immediate) return immediate.PublishImmediately(message);
            throw new NotSupportedException("This session does not support immediate dispatch.");
        }

        public static Task SendWithOptions<T>(this IMessageHandlerContext context, T message, SendOptions options) =>
            context.Send(message, options);

        public static Task Send<T>(this IMessageHandlerContext context, string destination, T message, SendOptions options) =>
            context.UniformSession.Send(destination, message, options);

        public static Task SendLocal<T>(this IMessageHandlerContext context, T message, SendOptions options) =>
            context.UniformSession.SendLocal(message, options);

        public static Task Publish<T>(this IMessageHandlerContext context, T message, PublishOptions options) =>
            context.UniformSession.Publish(message, options);

        public static Task Reply<T>(this IMessageHandlerContext context, T message, ReplyOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (context is IReplyWithOptions handler) return handler.ReplyWithOptions(message, options);
            if (options.Headers.Count == 0 && options.Destination == null && options.CorrelationId == null &&
                options.MessageId == null && options.ReplyAddress == null && !options.ImmediateDispatch)
                return context.Reply(message);
            throw new NotSupportedException("This handler context does not support reply options.");
        }
    }
}
