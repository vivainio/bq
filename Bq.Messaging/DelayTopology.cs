using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using RabbitMQ.Client;

namespace Bq.Messaging
{
    /// <summary>Which RabbitMQ mechanism holds a delayed message until it is due. The broker holds it either way; Bq does not poll the database.</summary>
    public enum DelayedDeliveryMode
    {
        /// <summary>Native when the broker is RabbitMQ 4.4 or later and the endpoint's input queue is a quorum queue, otherwise levels.</summary>
        Auto,
        /// <summary>The <c>bq.v1</c> delay levels, see <see cref="DelayTopology"/>. Works on any broker with quorum queues. A delayed publish is not possible.</summary>
        Levels,
        /// <summary>The <c>x-opt-delivery-time</c> header of RabbitMQ 4.4 and later. Needs quorum destination queues, because a classic queue ignores it.</summary>
        Native
    }

    /// <summary>
    /// Delayed delivery built from ordinary RabbitMQ exchanges and queues, for brokers without native delayed messages.
    /// There are 28 levels, one for each bit of a delay in whole seconds. Level <c>n</c> is a queue whose messages all expire after 2^n seconds,
    /// and an expired message is dead-lettered to the next level down. Because every message in a level has the same TTL, messages expire
    /// in order at the head of the queue, which a single queue with per-message TTLs cannot do.
    /// A delayed message is published to the exchange of its highest set bit, with a routing key made of the 28 bits followed by the
    /// destination queue. Each level exchange sends the message to its queue when its own bit is 1, or on to the next level when it is 0.
    /// The last exchange, <see cref="DeliveryExchange"/>, routes to the destination queue bound with <see cref="BindDestination"/>.
    /// </summary>
    public static class DelayTopology
    {
        public const int LevelCount = 28;
        public const string DeliveryExchange = "bq.v1.delay-delivery";

        /// <summary>The longest delay the levels can hold, about 8.5 years.</summary>
        public static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds((1L << LevelCount) - 1);

        public static string LevelName(int level)
        {
            if (level < 0 || level >= LevelCount) throw new ArgumentOutOfRangeException(nameof(level));
            return "bq.v1.delay-level-" + level.ToString("00", CultureInfo.InvariantCulture);
        }

        /// <summary>A delay in whole seconds, rounded up so a message is never delivered early. Must be positive and at most <see cref="MaxDelay"/>.</summary>
        public static long Seconds(TimeSpan delay)
        {
            if (delay <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(delay), "A delay must be positive.");
            if (delay > MaxDelay) throw new ArgumentOutOfRangeException(nameof(delay), "The longest delay is " + MaxDelay + ".");
            return (long)Math.Ceiling(delay.TotalSeconds);
        }

        /// <summary>The exchange a delayed message is published to: the level of the highest set bit.</summary>
        public static string EntryExchange(TimeSpan delay)
        {
            var seconds = Seconds(delay);
            var level = LevelCount - 1;
            while ((seconds >> level & 1) == 0) level--;
            return LevelName(level);
        }

        /// <summary>The 28 bits of the delay, highest first, then the destination queue, all separated by dots.</summary>
        public static string RoutingKey(TimeSpan delay, string destination)
        {
            if (string.IsNullOrEmpty(destination)) throw new ArgumentException("A destination is required.", nameof(destination));
            var seconds = Seconds(delay);
            var bits = new string[LevelCount + 1];
            for (var level = LevelCount - 1; level >= 0; level--) bits[LevelCount - 1 - level] = (seconds >> level & 1) == 1 ? "1" : "0";
            bits[LevelCount] = destination;
            return string.Join(".", bits);
        }

        /// <summary>The binding key of a destination on <see cref="DeliveryExchange"/>: any 28 bits, then its queue name.</summary>
        public static string DestinationBindingKey(string destination) =>
            string.Join(".", Enumerable.Repeat("*", LevelCount)) + "." + destination;

        /// <summary>Declares the level exchanges and queues and the delivery exchange. It is safe to call from every endpoint at start-up.</summary>
        public static void Declare(IModel channel, bool quorumQueues = true)
        {
            if (channel == null) throw new ArgumentNullException(nameof(channel));
            // Every exchange must exist before one is bound to another.
            channel.ExchangeDeclare(DeliveryExchange, ExchangeType.Topic, true);
            for (var level = LevelCount - 1; level >= 0; level--) channel.ExchangeDeclare(LevelName(level), ExchangeType.Topic, true);
            for (var level = LevelCount - 1; level >= 0; level--)
            {
                var name = LevelName(level);
                var next = level == 0 ? DeliveryExchange : LevelName(level - 1);
                var arguments = new Dictionary<string, object>
                {
                    ["x-message-ttl"] = (1L << level) * 1000L,
                    ["x-dead-letter-exchange"] = next
                };
                if (quorumQueues) arguments["x-queue-type"] = "quorum";
                channel.QueueDeclare(name, true, false, false, arguments);
                var skipped = string.Concat(Enumerable.Repeat("*.", LevelCount - 1 - level));
                channel.QueueBind(name, name, skipped + "1.#");
                channel.ExchangeBind(next, name, skipped + "0.#");
            }
        }

        /// <summary>Lets messages delayed for <paramref name="queue"/> come back to it. Call it for every queue that receives delayed messages.</summary>
        public static void BindDestination(IModel channel, string queue)
        {
            if (channel == null) throw new ArgumentNullException(nameof(channel));
            if (string.IsNullOrEmpty(queue)) throw new ArgumentException("A queue is required.", nameof(queue));
            channel.QueueBind(queue, DeliveryExchange, DestinationBindingKey(queue));
        }
    }
}
