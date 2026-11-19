using System;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public static class EndpointConfigurationExtensions
    {
        public static void LimitMessageProcessingConcurrencyTo(this EndpointConfiguration configuration, int maxConcurrency)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (maxConcurrency < 1 || maxConcurrency > ushort.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(maxConcurrency));
            configuration.MaxConcurrency = (ushort)maxConcurrency;
        }

        public static void SendFailedMessagesTo(this EndpointConfiguration configuration, string errorQueue)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (string.IsNullOrWhiteSpace(errorQueue)) throw new ArgumentException("Error queue is required.", nameof(errorQueue));
            configuration.Recoverability.ErrorQueue = errorQueue;
        }

        public static void DefineCriticalErrorAction(this EndpointConfiguration configuration, Func<ICriticalErrorContext, Task> onCriticalError)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.CriticalErrorAction = onCriticalError ?? throw new ArgumentNullException(nameof(onCriticalError));
        }

        public static RecoverabilityOptions Recoverability(this EndpointConfiguration configuration) =>
            (configuration ?? throw new ArgumentNullException(nameof(configuration))).Recoverability;
    }

    public sealed class RetryFailedSettings
    {
        internal Func<FailedMessage, Task> Notification { get; private set; }

        public RetryFailedSettings OnMessageSentToErrorQueue(Func<FailedMessage, Task> notificationCallback)
        {
            Notification = notificationCallback ?? throw new ArgumentNullException(nameof(notificationCallback));
            return this;
        }
    }

    public sealed class ImmediateRetriesSettings
    {
        internal int Count { get; private set; }
        internal Func<ImmediateRetryMessage, Task> Notification { get; private set; }
        internal ImmediateRetriesSettings(int count) { Count = count; }

        public void NumberOfRetries(int numberOfRetries)
        {
            if (numberOfRetries < 0) throw new ArgumentOutOfRangeException(nameof(numberOfRetries));
            Count = numberOfRetries;
        }

        public ImmediateRetriesSettings OnMessageBeingRetried(Func<ImmediateRetryMessage, Task> notificationCallback)
        {
            Notification = notificationCallback ?? throw new ArgumentNullException(nameof(notificationCallback));
            return this;
        }
    }

    public sealed class DelayedRetriesSettings
    {
        internal int Count { get; private set; }
        internal TimeSpan Increment { get; private set; }
        internal bool Changed { get; private set; }
        internal Func<DelayedRetryMessage, Task> Notification { get; private set; }

        internal DelayedRetriesSettings(TimeSpan[] delays)
        {
            Count = delays.Length;
            Increment = delays.Length == 0 ? TimeSpan.FromSeconds(10) : delays[0];
        }

        public DelayedRetriesSettings NumberOfRetries(int numberOfRetries)
        {
            if (numberOfRetries < 0) throw new ArgumentOutOfRangeException(nameof(numberOfRetries));
            Count = numberOfRetries;
            Changed = true;
            return this;
        }

        public DelayedRetriesSettings TimeIncrease(TimeSpan timeIncrease)
        {
            if (timeIncrease < TimeSpan.Zero || timeIncrease > TimeSpan.FromDays(24))
                throw new ArgumentOutOfRangeException(nameof(timeIncrease));
            Increment = timeIncrease;
            Changed = true;
            return this;
        }

        public DelayedRetriesSettings OnMessageBeingRetried(Func<DelayedRetryMessage, Task> notificationCallback)
        {
            Notification = notificationCallback ?? throw new ArgumentNullException(nameof(notificationCallback));
            return this;
        }
    }
}
