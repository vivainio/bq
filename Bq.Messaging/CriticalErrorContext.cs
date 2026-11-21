using System;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public interface ICriticalErrorContext
    {
        Func<Task> Stop { get; }
        string Error { get; }
        Exception Exception { get; }
    }

    public sealed class CriticalErrorContext : ICriticalErrorContext
    {
        public CriticalErrorContext(Func<Task> stop, string error, Exception exception)
        {
            Stop = stop ?? throw new ArgumentNullException(nameof(stop));
            Error = error ?? throw new ArgumentNullException(nameof(error));
            Exception = exception ?? throw new ArgumentNullException(nameof(exception));
        }

        public Func<Task> Stop { get; }
        public string Error { get; }
        public Exception Exception { get; }
    }
}

namespace Bq.Messaging
{
    /// <summary>Lets handlers and features report an unrecoverable condition to the endpoint's critical error action.</summary>
    public class CriticalError
    {
        readonly Func<ICriticalErrorContext, Task> onCriticalError;
        readonly Func<Task> stop;

        public CriticalError(Func<ICriticalErrorContext, Task> onCriticalError) : this(onCriticalError, () => Task.CompletedTask) { }

        internal CriticalError(Func<ICriticalErrorContext, Task> onCriticalError, Func<Task> stop)
        {
            this.onCriticalError = onCriticalError ?? throw new ArgumentNullException(nameof(onCriticalError));
            this.stop = stop;
        }

        public void Raise(string errorMessage, Exception exception)
        {
            if (string.IsNullOrWhiteSpace(errorMessage)) throw new ArgumentException("An error message is required.", nameof(errorMessage));
            if (exception == null) throw new ArgumentNullException(nameof(exception));
            // The callback runs on its own task so a stuck or slow action cannot block the reporting code.
            Task.Run(() => onCriticalError(new CriticalErrorContext(stop, errorMessage, exception)));
        }
    }
}
