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

        public void DelayDeliveryWith(TimeSpan value)
        {
            if (value < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(value));
            delay = value;
            deliverAt = null;
        }

        public void DoNotDeliverBefore(DateTimeOffset value)
        {
            deliverAt = value;
            delay = null;
        }

        internal DateTimeOffset? DueAt => delay.HasValue ? DateTimeOffset.UtcNow.Add(delay.Value) : deliverAt;
    }

    public sealed class PublishOptions : MessageOptions { }
    public sealed class ReplyOptions : MessageOptions { }

    public static class MessageOptionsExtensions
    {
        public static Task SendWithOptions<T>(this IUniformSession session, T message, SendOptions options)
        {
            var dueAt = options.DueAt;
            session = options.Apply(session);
            return dueAt.HasValue ? session.SendAt(message, dueAt.Value) : session.Send(message);
        }

        public static Task Send<T>(this IUniformSession session, string destination, T message, SendOptions options)
        {
            var dueAt = options.DueAt;
            session = options.Apply(session);
            return dueAt.HasValue ? session.SendAt(destination, message, dueAt.Value) : session.Send(destination, message);
        }

        public static Task SendLocal<T>(this IUniformSession session, T message, SendOptions options)
        {
            var dueAt = options.DueAt;
            session = options.Apply(session);
            return dueAt.HasValue ? session.SendLocalAt(message, dueAt.Value) : session.SendLocal(message);
        }

        public static Task Publish<T>(this IUniformSession session, T message, PublishOptions options) =>
            options.Apply(session).Publish(message);

        public static Task SendWithOptions<T>(this IMessageHandlerContext context, T message, SendOptions options) =>
            context.UniformSession.SendWithOptions(message, options);

        public static Task Send<T>(this IMessageHandlerContext context, string destination, T message, SendOptions options) =>
            context.UniformSession.Send(destination, message, options);

        public static Task SendLocal<T>(this IMessageHandlerContext context, T message, SendOptions options) =>
            context.UniformSession.SendLocal(message, options);

        public static Task Publish<T>(this IMessageHandlerContext context, T message, PublishOptions options) =>
            context.UniformSession.Publish(message, options);

        public static Task Reply<T>(this IMessageHandlerContext context, T message, ReplyOptions options)
        {
            if (options.Headers.Count == 0) return context.Reply(message);
            if (context is RabbitEndpoint.HandlerContext handler) return handler.ReplyWithHeaders(message, options.Headers);
            throw new NotSupportedException("This handler context does not support reply headers.");
        }
    }
}
