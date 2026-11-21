using System;
using System.Threading.Tasks;
using Bq.Messaging.Faults;

namespace Bq.Messaging
{
    public class Notifications
    {
        public ErrorsNotifications Errors { get; } = new ErrorsNotifications();
    }
}

namespace Bq.Messaging.Faults
{
    public static class FaultsHeaderKeys
    {
        /// <summary>The endpoint queue the message failed in. Bq writes this on failed message copies.</summary>
        public const string FailedQ = "x-bq-failed-endpoint";
    }

    public class ErrorsNotifications
    {
        public event EventHandler<FailedMessage> MessageSentToErrorQueue;
        public event EventHandler<ImmediateRetryMessage> MessageHasFailedAnImmediateRetryAttempt;
        public event EventHandler<DelayedRetryMessage> MessageHasBeenSentToDelayedRetries;

        internal bool HasErrorQueueHandlers => MessageSentToErrorQueue != null;
        internal bool HasImmediateRetryHandlers => MessageHasFailedAnImmediateRetryAttempt != null;
        internal bool HasDelayedRetryHandlers => MessageHasBeenSentToDelayedRetries != null;
        internal void Raise(FailedMessage message) => MessageSentToErrorQueue?.Invoke(this, message);
        internal void Raise(ImmediateRetryMessage message) => MessageHasFailedAnImmediateRetryAttempt?.Invoke(this, message);
        internal void Raise(DelayedRetryMessage message) => MessageHasBeenSentToDelayedRetries?.Invoke(this, message);
    }
}
