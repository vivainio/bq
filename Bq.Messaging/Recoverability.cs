using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public sealed class RecoverabilityOptions
    {
        public int ImmediateRetries { get; set; } = 2;
        public TimeSpan[] DelayedRetries { get; set; } = { TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(30) };
        public string ErrorQueue { get; set; }
        public Func<Exception, bool> IsUnrecoverable { get; set; }
        public Func<RecoveryContext, RecoveryAction> Policy { get; set; }
        internal Func<RecoverabilityConfig, ErrorContext, RecoverabilityAction> CompatiblePolicy { get; private set; }
        internal Func<ImmediateRetryMessage, Task> ImmediateRetryNotification { get; private set; }
        internal Func<DelayedRetryMessage, Task> DelayedRetryNotification { get; private set; }
        internal Func<FailedMessage, Task> ErrorQueueNotification { get; private set; }

        internal HashSet<Type> UnrecoverableExceptionTypes { get; } = new HashSet<Type>();

        public RecoverabilityOptions AddUnrecoverableException<T>() where T : Exception =>
            AddUnrecoverableException(typeof(T));

        public RecoverabilityOptions AddUnrecoverableException(Type exceptionType)
        {
            if (exceptionType == null) throw new ArgumentNullException(nameof(exceptionType));
            if (!typeof(Exception).IsAssignableFrom(exceptionType))
                throw new ArgumentException("The type must derive from Exception.", nameof(exceptionType));
            UnrecoverableExceptionTypes.Add(exceptionType);
            return this;
        }

        public RecoverabilityOptions CustomPolicy(Func<RecoverabilityConfig, ErrorContext, RecoverabilityAction> custom)
        {
            CompatiblePolicy = custom ?? throw new ArgumentNullException(nameof(custom));
            return this;
        }

        public RecoverabilityOptions Failed(Action<RetryFailedSettings> customizations)
        {
            if (customizations == null) throw new ArgumentNullException(nameof(customizations));
            var settings = new RetryFailedSettings();
            customizations(settings);
            ErrorQueueNotification = settings.Notification ?? ErrorQueueNotification;
            return this;
        }

        public RecoverabilityOptions Immediate(Action<ImmediateRetriesSettings> customizations)
        {
            if (customizations == null) throw new ArgumentNullException(nameof(customizations));
            var settings = new ImmediateRetriesSettings(ImmediateRetries);
            customizations(settings);
            ImmediateRetries = settings.Count;
            ImmediateRetryNotification = settings.Notification ?? ImmediateRetryNotification;
            return this;
        }

        public RecoverabilityOptions Delayed(Action<DelayedRetriesSettings> customizations)
        {
            if (customizations == null) throw new ArgumentNullException(nameof(customizations));
            var settings = new DelayedRetriesSettings(DelayedRetries ?? Array.Empty<TimeSpan>());
            customizations(settings);
            if (settings.Changed)
            {
                var ticks = settings.Increment.Ticks;
                if (ticks > 0 && settings.Count > TimeSpan.FromDays(24).Ticks / ticks)
                    throw new ArgumentOutOfRangeException(nameof(customizations), "Delayed retry intervals cannot exceed 24 days.");
                var delays = new TimeSpan[settings.Count];
                for (var index = 0; index < delays.Length; index++)
                    delays[index] = TimeSpan.FromTicks(ticks * (index + 1));
                DelayedRetries = delays;
            }
            DelayedRetryNotification = settings.Notification ?? DelayedRetryNotification;
            return this;
        }
    }

    public class ImmediateRetryMessage
    {
        public ImmediateRetryMessage(string messageId, Dictionary<string, string> headers, byte[] body,
            Exception exception, int retryAttempt)
        {
            MessageId = messageId;
            Headers = headers;
            Body = body;
            Exception = exception;
            RetryAttempt = retryAttempt;
        }

        public string MessageId { get; }
        public Dictionary<string, string> Headers { get; }
        public byte[] Body { get; }
        public Exception Exception { get; }
        public int RetryAttempt { get; }
    }

    public class DelayedRetryMessage
    {
        public DelayedRetryMessage(string messageId, Dictionary<string, string> headers, byte[] body,
            Exception exception, int retryAttempt)
        {
            MessageId = messageId;
            Headers = headers;
            Body = body;
            Exception = exception;
            RetryAttempt = retryAttempt;
        }

        public string MessageId { get; }
        public Dictionary<string, string> Headers { get; }
        public byte[] Body { get; }
        public Exception Exception { get; }
        public int RetryAttempt { get; }
    }

    public class FailedMessage
    {
        public FailedMessage(string messageId, Dictionary<string, string> headers, byte[] body, Exception exception, string errorQueue)
        {
            MessageId = messageId;
            Headers = headers;
            Body = body;
            Exception = exception;
            ErrorQueue = errorQueue;
        }

        public string MessageId { get; }
        public Dictionary<string, string> Headers { get; }
        public byte[] Body { get; }
        public Exception Exception { get; }
        public string ErrorQueue { get; }
    }

    public sealed class RecoveryContext
    {
        internal RecoveryContext(Exception exception, int immediateRetries, int delayedRetries,
            IDictionary<string, object> headers)
        {
            Exception = exception;
            ImmediateRetries = immediateRetries;
            DelayedRetries = delayedRetries;
            Headers = new System.Collections.ObjectModel.ReadOnlyDictionary<string, object>(
                new Dictionary<string, object>(headers ?? new Dictionary<string, object>()));
        }

        public Exception Exception { get; }
        public int ImmediateRetries { get; }
        public int DelayedRetries { get; }
        public IReadOnlyDictionary<string, object> Headers { get; }
    }

    public sealed class RecoveryAction
    {
        internal TimeSpan? Delay { get; }
        internal bool IsImmediate { get; }
        internal string ErrorQueue { get; }
        internal string DiscardReason { get; }

        RecoveryAction(TimeSpan? delay, bool immediate = false, string errorQueue = null, string discardReason = null)
        {
            Delay = delay;
            IsImmediate = immediate;
            ErrorQueue = errorQueue;
            DiscardReason = discardReason;
        }

        public static RecoveryAction ImmediateRetry() => new RecoveryAction(null, true);
        public static RecoveryAction MoveToError() => new RecoveryAction(null);
        internal static RecoveryAction MoveToError(string queue) =>
            new RecoveryAction(null, errorQueue: string.IsNullOrWhiteSpace(queue)
                ? throw new ArgumentException("Error queue is required.", nameof(queue)) : queue);
        internal static RecoveryAction Drop(string reason) =>
            new RecoveryAction(null, discardReason: reason ?? throw new ArgumentNullException(nameof(reason)));
        public static RecoveryAction DelayedRetry(TimeSpan delay)
        {
            if (delay < TimeSpan.Zero || delay > TimeSpan.FromDays(24)) throw new ArgumentOutOfRangeException(nameof(delay));
            return new RecoveryAction(delay);
        }
    }
    // Payload is an encoded AMQP message snapshot. RecoveryId uniquely identifies
    // one failed delivery cycle, allowing a handoff to be committed only once.
    public sealed class RecoveryRecord
    {
        public string RecoveryId { get; set; }
        public string MessageId { get; set; }
        public int Cycle { get; set; }
        public bool IsError { get; set; }
        // True once persistence records the broker confirmation.
        public bool Dispatched { get; set; }
        public string ErrorQueue { get; set; }
        public string ExceptionType { get; set; }
        public string ExceptionMessage { get; set; }
        public string ExceptionDetail { get; set; }
        public DateTime FailedAtUtc { get; set; }
        public DateTime DueAtUtc { get; set; }
        public byte[] Payload { get; set; }
    }
    public interface IRecoverySession
    {
        Task<bool> HasRecovery(string endpoint, string recoveryId);
        Task StoreRecovery(string endpoint, RecoveryRecord record);
    }
    public interface IRecoveryPersistence
    {
        Task<RecoveryRecord> NextRecovery(string endpoint);
        Task MarkRecoveryDispatched(string endpoint, string recoveryId);
        Task<RecoveryRecord> GetError(string endpoint, string recoveryId);
        Task<IReadOnlyList<RecoveryRecord>> ListErrors(string endpoint, int limit);
        // Atomically enqueue a replay and mark the error replayed. The caller supplies
        // a new delivery identity while retaining the original identity in metadata.
        Task<bool> ReplayError(string endpoint, string recoveryId, RecoveryRecord replay);
    }
}
