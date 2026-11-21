using System;
using System.Collections.Generic;
using System.Linq;

namespace Bq.Messaging.DeliveryConstraints
{
    public class DeliveryConstraint { }

    /// <summary>Reads and writes the delivery constraints of an outgoing operation. Bq fills them in before outgoing behaviors run.</summary>
    public static class DeliveryConstraintContextExtensions
    {
        const string Key = "Bq.DeliveryConstraints";

        public static void AddDeliveryConstraint(this ContextBag context, DeliveryConstraint constraint) =>
            Constraints(context ?? throw new ArgumentNullException(nameof(context))).Add(constraint ?? throw new ArgumentNullException(nameof(constraint)));

        public static bool TryGetDeliveryConstraint<T>(this ContextBag context, out T constraint) where T : DeliveryConstraint
        {
            constraint = Constraints(context ?? throw new ArgumentNullException(nameof(context))).OfType<T>().FirstOrDefault();
            return constraint != null;
        }

        public static List<DeliveryConstraint> GetDeliveryConstraints(this ContextBag context) =>
            Constraints(context ?? throw new ArgumentNullException(nameof(context)));

        public static void RemoveDeliveryConstraint(this ContextBag context, DeliveryConstraint constraint) =>
            Constraints(context ?? throw new ArgumentNullException(nameof(context))).Remove(constraint);

        public static void RemoveDeliveryConstaint(this ContextBag context, DeliveryConstraint constraint) => context.RemoveDeliveryConstraint(constraint);

        public static bool TryRemoveDeliveryConstraint<T>(this ContextBag context, out T constraint) where T : DeliveryConstraint
        {
            if (!context.TryGetDeliveryConstraint(out constraint)) return false;
            context.RemoveDeliveryConstraint(constraint);
            return true;
        }

        static List<DeliveryConstraint> Constraints(ContextBag context)
        {
            // Constraints belong to one operation, so they are stored on the bag itself, not inherited from a parent.
            if (!context.TryGetLocal(Key, out List<DeliveryConstraint> list)) context.SetLocal(Key, list = new List<DeliveryConstraint>());
            return list;
        }
    }
}

namespace Bq.Messaging.DelayedDelivery
{
    using Bq.Messaging.DeliveryConstraints;

    public class DelayedDeliveryConstraint : DeliveryConstraint { }

    public sealed class DelayDeliveryWith : DelayedDeliveryConstraint
    {
        public DelayDeliveryWith(TimeSpan delay) { Delay = delay; }
        public TimeSpan Delay { get; }
    }

    public sealed class DoNotDeliverBefore : DelayedDeliveryConstraint
    {
        public DoNotDeliverBefore(DateTime at) { At = at; }
        public DateTime At { get; }
    }

    public static class ExternalTimeoutManagerConfigurationExtensions
    {
        public static void UseExternalTimeoutManager(this EndpointConfiguration configuration, string externalTimeoutManagerAddress) =>
            throw new NotSupportedException("Bq stores delayed messages in its persistence provider. There is no timeout manager endpoint.");
    }
}

namespace Bq.Messaging.Performance.TimeToBeReceived
{
    using Bq.Messaging.DeliveryConstraints;

    public sealed class DiscardIfNotReceivedBefore : DeliveryConstraint
    {
        public DiscardIfNotReceivedBefore(TimeSpan maxTime) { MaxTime = maxTime; }
        public TimeSpan MaxTime { get; }
    }
}
