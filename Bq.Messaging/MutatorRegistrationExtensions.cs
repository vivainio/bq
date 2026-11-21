using System;

namespace Bq.Messaging
{
    public static class MutatorRegistrationExtensions
    {
        public static void RegisterMessageMutator(this EndpointConfiguration endpointConfiguration, object messageMutator)
        {
            if (endpointConfiguration == null) throw new ArgumentNullException(nameof(endpointConfiguration));
            if (messageMutator == null) throw new ArgumentNullException(nameof(messageMutator));

            var incoming = messageMutator as IMutateIncomingMessages;
            var incomingTransport = messageMutator as IMutateIncomingTransportMessages;
            var outgoing = messageMutator as IMutateOutgoingMessages;
            var outgoingTransport = messageMutator as IMutateOutgoingTransportMessages;
            if (incoming == null && incomingTransport == null && outgoing == null && outgoingTransport == null)
                throw new ArgumentException("A supported message mutator is required.", nameof(messageMutator));

            if (incomingTransport != null && !endpointConfiguration.IncomingTransportMutators.Contains(incomingTransport))
                endpointConfiguration.IncomingTransportMutators.Add(incomingTransport);
            if (outgoing != null && !endpointConfiguration.OutgoingMessageMutators.Contains(outgoing))
                endpointConfiguration.OutgoingMessageMutators.Add(outgoing);
            if (outgoingTransport != null && !endpointConfiguration.OutgoingTransportMutators.Contains(outgoingTransport))
                endpointConfiguration.OutgoingTransportMutators.Add(outgoingTransport);
            if (incoming == null || !endpointConfiguration.IncomingMessageMutators.Add(incoming)) return;

            endpointConfiguration.Incoming.Use(async (context, next) =>
            {
                var mutation = new MutateIncomingMessageContext(context.Message, context.LogicalHeaders);
                await incoming.MutateIncoming(mutation).ConfigureAwait(false);
                context.UpdateMessageInstance(mutation.Message);
                await next().ConfigureAwait(false);
            });
        }
    }
}
