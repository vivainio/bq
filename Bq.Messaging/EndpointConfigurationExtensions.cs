using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public static class EndpointConfigurationExtensions
    {
        public static AutoSubscribeSettings AutoSubscribe(this EndpointConfiguration config) =>
            (config ?? throw new ArgumentNullException(nameof(config))).AutoSubscriptionSettings;

        public static void DoNotCreateQueues(this EndpointConfiguration config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            config.UsePreProvisionedTopology = true;
        }

        public static void AddHeaderToAllOutgoingMessages(this EndpointConfiguration config, string key, string value)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (value == null) throw new ArgumentNullException(nameof(value));
            MessageOptions.ValidateHeaderName(key, nameof(key));
            config.OutgoingHeaders[key] = value;
        }

        public static void PurgeOnStartup(this EndpointConfiguration config, bool value)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            config.PurgeInputQueueOnStartup = value;
        }

        public static void OverridePublicReturnAddress(this EndpointConfiguration configuration, string address)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.PublicReturnAddress = MessageOptions.ValidateAddressField(address, nameof(address));
        }

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

    public sealed class AutoSubscribeSettings
    {
        readonly HashSet<Type> excluded = new HashSet<Type>();

        internal AutoSubscribeSettings() { }

        public AutoSubscribeSettings DisableFor<T>() => DisableFor(typeof(T));

        public AutoSubscribeSettings DisableFor(Type eventType)
        {
            excluded.Add(eventType ?? throw new ArgumentNullException(nameof(eventType)));
            return this;
        }

        internal bool IsDisabled(Type eventType) => excluded.Contains(eventType);
        internal bool SagaSubscriptionsDisabled { get; private set; }

        public void DoNotAutoSubscribeSagas() => SagaSubscriptionsDisabled = true;
    }

    public sealed class RetryFailedSettings
    {
        internal Func<FailedMessage, Task> Notification { get; private set; }
        internal Action<Dictionary<string, string>> CustomizeHeaders { get; private set; }

        public RetryFailedSettings HeaderCustomization(Action<Dictionary<string, string>> customization)
        {
            CustomizeHeaders = customization ?? throw new ArgumentNullException(nameof(customization));
            return this;
        }

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
