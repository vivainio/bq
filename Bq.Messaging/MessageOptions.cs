using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public abstract class MessageOptions
    {
        internal IDictionary<string, object> Headers { get; } = new Dictionary<string, object>();

        public void SetHeader(string name, object value)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Header name is required.", nameof(name));
            Headers[name] = value;
        }

        internal IUniformSession Apply(IUniformSession session)
        {
            foreach (var header in Headers) session = session.WithHeader(header.Key, header.Value);
            return session;
        }
    }

    public sealed class SendOptions : MessageOptions
    {
        TimeSpan? delay;
        DateTimeOffset? deliverAt;
        internal string Destination { get; private set; }
        internal bool ImmediateDispatch { get; private set; }

        public void SetDestination(string destination)
        {
            if (string.IsNullOrWhiteSpace(destination)) throw new ArgumentException("Destination is required.", nameof(destination));
            Destination = destination;
        }

        public void RequireImmediateDispatch()
        {
            if (delay.HasValue || deliverAt.HasValue) throw new InvalidOperationException("Immediate dispatch cannot be scheduled.");
            ImmediateDispatch = true;
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
    }

    public sealed class PublishOptions : MessageOptions { }
    public sealed class ReplyOptions : MessageOptions { }

    internal interface IReplyWithHeaders
    {
        Task ReplyWithHeaders<T>(T message, IDictionary<string, object> headers);
    }

    internal interface IImmediateDispatch
    {
        Task SendImmediately<T>(T message, string destination, bool local);
    }

    public static class MessageOptionsExtensions
    {
        internal static T Initialize<T>(Action<T> initialize) where T : class, new()
        {
            if (initialize == null) throw new ArgumentNullException(nameof(initialize));
            var message = new T();
            initialize(message);
            return message;
        }

        public static Task Send<T>(this IMessageSession session, Action<T> initialize) where T : class, new() =>
            session.Send(initialize, new SendOptions());

        public static Task Publish<T>(this IMessageSession session, Action<T> initialize) where T : class, new() =>
            session.Publish(initialize, new PublishOptions());

        public static Task Send<T>(this IMessageHandlerContext context, Action<T> initialize) where T : class, new() =>
            context.Send(initialize, new SendOptions());

        public static Task Publish<T>(this IMessageHandlerContext context, Action<T> initialize) where T : class, new() =>
            context.Publish(initialize, new PublishOptions());

        public static Task Reply<T>(this IMessageHandlerContext context, Action<T> initialize) where T : class, new() =>
            context.Reply(initialize, new ReplyOptions());

        public static Task Send<T>(this IUniformSession session, Action<T> initialize) where T : class, new() =>
            session.Send(initialize, new SendOptions());

        public static Task Publish<T>(this IUniformSession session, Action<T> initialize) where T : class, new() =>
            session.Publish(initialize, new PublishOptions());

        public static Task SendWithOptions<T>(this IUniformSession session, T message, SendOptions options) =>
            session.Send(message, options);

        internal static Task SendUsingOptions<T>(IUniformSession session, T message, SendOptions options,
            string destination = null, bool local = false)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if ((destination != null || local) && options.Destination != null)
                throw new ArgumentException("A destination has already been supplied.", nameof(options));
            destination = destination ?? options.Destination;
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

        public static Task Publish<T>(this IUniformSession session, T message, PublishOptions options) =>
            options.Apply(session).Publish(message);

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
            if (options.Headers.Count == 0) return context.Reply(message);
            if (context is IReplyWithHeaders handler) return handler.ReplyWithHeaders(message, options.Headers);
            throw new NotSupportedException("This handler context does not support reply headers.");
        }
    }
}
