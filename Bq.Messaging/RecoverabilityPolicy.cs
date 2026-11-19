using System;
using System.Collections.Generic;
using System.Linq;

namespace Bq.Messaging
{
    public sealed class ImmediateConfig
    {
        public ImmediateConfig(int maxNumberOfRetries) { MaxNumberOfRetries = maxNumberOfRetries; }
        public int MaxNumberOfRetries { get; }
    }

    public sealed class DelayedConfig
    {
        readonly TimeSpan[] delays;

        public DelayedConfig(int maxNumberOfRetries, TimeSpan timeIncrease)
        {
            MaxNumberOfRetries = maxNumberOfRetries;
            TimeIncrease = timeIncrease;
        }

        internal DelayedConfig(TimeSpan[] delays) : this(delays.Length, delays.Length == 0 ? TimeSpan.Zero : delays[0])
        {
            this.delays = (TimeSpan[])delays.Clone();
        }

        public int MaxNumberOfRetries { get; }
        public TimeSpan TimeIncrease { get; }
        internal TimeSpan DelayAt(int index) => delays == null
            ? TimeSpan.FromTicks(checked(TimeIncrease.Ticks * (index + 1L))) : delays[index];
    }

    public sealed class FailedConfig
    {
        public FailedConfig(string errorQueue, HashSet<Type> unrecoverableExceptionTypes)
        {
            ErrorQueue = errorQueue;
            UnrecoverableExceptionTypes = new HashSet<Type>(unrecoverableExceptionTypes ??
                throw new ArgumentNullException(nameof(unrecoverableExceptionTypes)));
        }

        public string ErrorQueue { get; }
        public HashSet<Type> UnrecoverableExceptionTypes { get; }
    }

    public sealed class RecoverabilityConfig
    {
        public RecoverabilityConfig(ImmediateConfig immediateConfig, DelayedConfig delayedConfig, FailedConfig failedConfig)
        {
            Immediate = immediateConfig ?? throw new ArgumentNullException(nameof(immediateConfig));
            Delayed = delayedConfig ?? throw new ArgumentNullException(nameof(delayedConfig));
            Failed = failedConfig ?? throw new ArgumentNullException(nameof(failedConfig));
        }

        public ImmediateConfig Immediate { get; }
        public DelayedConfig Delayed { get; }
        public FailedConfig Failed { get; }
    }

    public sealed class IncomingMessage
    {
        internal IncomingMessage(string messageId, IDictionary<string, object> headers, ReadOnlyMemory<byte> body)
        {
            MessageId = messageId;
            Headers = MessageHeaders.AsText(headers ?? new Dictionary<string, object>())
                .ToDictionary(header => header.Key, header => header.Value);
            Body = body.ToArray();
        }

        public string MessageId { get; }
        public string NativeMessageId => MessageId;
        public Dictionary<string, string> Headers { get; }
        public byte[] Body { get; }
    }

    public sealed class ErrorContext
    {
        internal ErrorContext(Exception exception, int attempt, int cycle, IncomingMessage message, ReadOnlyContextBag extensions)
        {
            Exception = exception;
            ImmediateProcessingFailures = attempt + 1;
            DelayedDeliveriesPerformed = cycle;
            Message = message;
            Extensions = extensions ?? new ContextBag();
        }

        public Exception Exception { get; }
        public int ImmediateProcessingFailures { get; }
        public int DelayedDeliveriesPerformed { get; }
        public IncomingMessage Message { get; }
        public ReadOnlyContextBag Extensions { get; }
    }

    public abstract class RecoverabilityAction
    {
        internal abstract RecoveryAction ToBqAction();

        public static ImmediateRetry ImmediateRetry() => new ImmediateRetry();
        public static DelayedRetry DelayedRetry(TimeSpan timeSpan) => new DelayedRetry(timeSpan);
        public static MoveToError MoveToError(string errorQueue) => new MoveToError(errorQueue);
        public static Discard Discard(string reason) => new Discard(reason);
    }

    public sealed class ImmediateRetry : RecoverabilityAction
    {
        internal ImmediateRetry() { }
        internal override RecoveryAction ToBqAction() => RecoveryAction.ImmediateRetry();
    }

    public sealed class DelayedRetry : RecoverabilityAction
    {
        internal DelayedRetry(TimeSpan delay) { Delay = delay; }
        public TimeSpan Delay { get; }
        internal override RecoveryAction ToBqAction() => RecoveryAction.DelayedRetry(Delay);
    }

    public sealed class MoveToError : RecoverabilityAction
    {
        internal MoveToError(string errorQueue) { ErrorQueue = errorQueue; }
        public string ErrorQueue { get; }
        internal override RecoveryAction ToBqAction() => RecoveryAction.MoveToError(ErrorQueue);
    }

    public sealed class Discard : RecoverabilityAction
    {
        internal Discard(string reason) { Reason = reason; }
        public string Reason { get; }
        internal override RecoveryAction ToBqAction() => RecoveryAction.Drop(Reason);
    }

    public static class DefaultRecoverabilityPolicy
    {
        public static RecoverabilityAction Invoke(RecoverabilityConfig config, ErrorContext errorContext)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (errorContext == null) throw new ArgumentNullException(nameof(errorContext));
            if (config.Failed.UnrecoverableExceptionTypes.Any(type => type.IsInstanceOfType(errorContext.Exception)))
                return RecoverabilityAction.MoveToError(config.Failed.ErrorQueue);
            if (errorContext.ImmediateProcessingFailures <= config.Immediate.MaxNumberOfRetries)
                return RecoverabilityAction.ImmediateRetry();
            return errorContext.DelayedDeliveriesPerformed < config.Delayed.MaxNumberOfRetries
                ? (RecoverabilityAction)RecoverabilityAction.DelayedRetry(config.Delayed.DelayAt(errorContext.DelayedDeliveriesPerformed))
                : RecoverabilityAction.MoveToError(config.Failed.ErrorQueue);
        }
    }
}
