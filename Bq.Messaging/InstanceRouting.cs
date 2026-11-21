using System;

namespace Bq.Messaging
{
    /// <summary>
    /// Instance routing options carry marker values that the endpoint resolves when it prepares the message,
    /// since only the endpoint knows its own instance address and the route of the message type.
    /// </summary>
    internal static class InstanceRouting
    {
        public const string ThisInstance = "x-bq-route-this-instance";
        public const string SpecificInstancePrefix = "x-bq-route-instance:";
        public const string ReplyThisInstance = "x-bq-reply-this-instance";
        public const string ReplyAnyInstance = "x-bq-reply-any-instance";

        public static string InstanceAddress(string endpointAddress, string instanceId) => endpointAddress + "-" + instanceId;
    }

    public static class RoutingOptionInstanceExtensions
    {
        public static void RouteToThisInstance(this SendOptions options) =>
            (options ?? throw new ArgumentNullException(nameof(options))).SetDestination(InstanceRouting.ThisInstance);

        public static void RouteToSpecificInstance(this SendOptions options, string instanceId)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (string.IsNullOrWhiteSpace(instanceId)) throw new ArgumentException("An instance ID is required.", nameof(instanceId));
            options.SetDestination(InstanceRouting.SpecificInstancePrefix + instanceId);
        }

        public static void RouteReplyToThisInstance(this SendOptions options) =>
            (options ?? throw new ArgumentNullException(nameof(options))).RouteReplyTo(InstanceRouting.ReplyThisInstance);

        public static void RouteReplyToAnyInstance(this SendOptions options) =>
            (options ?? throw new ArgumentNullException(nameof(options))).RouteReplyTo(InstanceRouting.ReplyAnyInstance);

        public static void RouteReplyToThisInstance(this ReplyOptions options) =>
            (options ?? throw new ArgumentNullException(nameof(options))).RouteReplyTo(InstanceRouting.ReplyThisInstance);

        public static void RouteReplyToAnyInstance(this ReplyOptions options) =>
            (options ?? throw new ArgumentNullException(nameof(options))).RouteReplyTo(InstanceRouting.ReplyAnyInstance);

        public static bool IsRoutingToThisInstance(this SendOptions options) => options.GetDestination() == InstanceRouting.ThisInstance;

        public static string GetRouteToSpecificInstance(this SendOptions options)
        {
            var destination = options.GetDestination();
            return destination != null && destination.StartsWith(InstanceRouting.SpecificInstancePrefix, StringComparison.Ordinal)
                ? destination.Substring(InstanceRouting.SpecificInstancePrefix.Length) : null;
        }

        public static bool IsRoutingReplyToThisInstance(this SendOptions options) => options.GetReplyToRoute() == InstanceRouting.ReplyThisInstance;
        public static bool IsRoutingReplyToThisInstance(this ReplyOptions options) => options.GetReplyToRoute() == InstanceRouting.ReplyThisInstance;
        public static bool IsRoutingReplyToAnyInstance(this SendOptions options) => options.GetReplyToRoute() == InstanceRouting.ReplyAnyInstance;
        public static bool IsRoutingReplyToAnyInstance(this ReplyOptions options) => options.GetReplyToRoute() == InstanceRouting.ReplyAnyInstance;
    }

    public sealed partial class EndpointConfiguration
    {
        internal string InstanceDiscriminator { get; private set; }

        /// <summary>
        /// Gives this instance its own input queue, named after the endpoint queue and the discriminator, in addition to the
        /// shared endpoint queue that all instances compete on.
        /// </summary>
        public void MakeInstanceUniquelyAddressable(string discriminator)
        {
            if (string.IsNullOrWhiteSpace(discriminator)) throw new ArgumentException("A discriminator is required.", nameof(discriminator));
            InstanceDiscriminator = discriminator;
        }
    }
}
