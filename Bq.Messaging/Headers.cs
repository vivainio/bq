namespace Bq.Messaging
{
    /// <summary>NServiceBus header names. Values Bq writes itself reuse the names on its wire.</summary>
    public static class Headers
    {
        public const string HttpFrom = "NServiceBus.From";
        public const string HttpTo = "NServiceBus.To";
        public const string RouteTo = "NServiceBus.Header.RouteTo";
        public const string DestinationSites = "NServiceBus.DestinationSites";
        public const string OriginatingSite = "NServiceBus.OriginatingSite";
        public const string SagaId = "NServiceBus.SagaId";
        public const string MessageId = "NServiceBus.MessageId";
        public const string CorrelationId = "NServiceBus.CorrelationId";
        public const string ReplyToAddress = "NServiceBus.ReplyToAddress";
        public const string HeaderName = "Header";
        public const string NServiceBusVersion = "NServiceBus.Version";
        public const string ReturnMessageErrorCodeHeader = "NServiceBus.ReturnMessage.ErrorCode";
        public const string ControlMessageHeader = "NServiceBus.ControlMessage";
        public const string SagaType = "NServiceBus.SagaType";
        public const string OriginatingSagaId = "NServiceBus.OriginatingSagaId";
        public const string OriginatingSagaType = "NServiceBus.OriginatingSagaType";
        public const string DelayedRetries = "NServiceBus.Retries";
        public const string DelayedRetriesTimestamp = "NServiceBus.Retries.Timestamp";
        public const string ImmediateRetries = "NServiceBus.FLRetries";
        public const string ProcessingStarted = "NServiceBus.ProcessingStarted";
        public const string ProcessingEnded = "NServiceBus.ProcessingEnded";
        public const string TimeSent = MessageHeaders.TimeSent;
        public const string DeliverAt = "NServiceBus.DeliverAt";
        public const string RelatedTo = MessageHeaders.RelatedTo;
        public const string EnclosedMessageTypes = "NServiceBus.EnclosedMessageTypes";
        public const string ContentType = "NServiceBus.ContentType";
        public const string SubscriptionMessageType = "SubscriptionMessageType";
        public const string SubscriberTransportAddress = "NServiceBus.SubscriberAddress";
        public const string SubscriberEndpoint = "NServiceBus.SubscriberEndpoint";
        public const string IsSagaTimeoutMessage = "NServiceBus.IsSagaTimeoutMessage";
        public const string IsDeferredMessage = MessageHeaders.Deferred;
        public const string OriginatingEndpoint = "NServiceBus.OriginatingEndpoint";
        public const string OriginatingMachine = "NServiceBus.OriginatingMachine";
        public const string OriginatingHostId = "$.diagnostics.originating.hostid";
        public const string ProcessingEndpoint = "NServiceBus.ProcessingEndpoint";
        public const string ProcessingMachine = "NServiceBus.ProcessingMachine";
        public const string HostDisplayName = "$.diagnostics.hostdisplayname";
        public const string HostId = "$.diagnostics.hostid";
        public const string HasLicenseExpired = "$.diagnostics.license.expired";
        public const string OriginatingAddress = "NServiceBus.OriginatingAddress";
        public const string ConversationId = MessageHeaders.ConversationId;
        public const string PreviousConversationId = MessageHeaders.PreviousConversationId;
        public const string MessageIntent = "NServiceBus.MessageIntent";
        public const string TimeToBeReceived = "NServiceBus.TimeToBeReceived";
        public const string NonDurableMessage = "NServiceBus.NonDurableMessage";
    }
}
