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
