using System;
using System.Diagnostics;

namespace Bq.Messaging.Logging
{
    public enum LogLevel { Debug, Info, Warn, Error, Fatal }

    public interface ILog
    {
        bool IsDebugEnabled { get; }
        bool IsInfoEnabled { get; }
        bool IsWarnEnabled { get; }
        bool IsErrorEnabled { get; }
        bool IsFatalEnabled { get; }
        void Debug(string message);
        void Debug(string message, Exception exception);
        void DebugFormat(string format, params object[] args);
        void Info(string message);
        void Info(string message, Exception exception);
        void InfoFormat(string format, params object[] args);
        void Warn(string message);
        void Warn(string message, Exception exception);
        void WarnFormat(string format, params object[] args);
        void Error(string message);
        void Error(string message, Exception exception);
        void ErrorFormat(string format, params object[] args);
        void Fatal(string message);
        void Fatal(string message, Exception exception);
        void FatalFormat(string format, params object[] args);
    }

    public interface ILoggerFactory
    {
        ILog GetLogger(Type type);
        ILog GetLogger(string name);
    }

    public abstract class LoggingFactoryDefinition
    {
        protected internal abstract ILoggerFactory GetLoggingFactory();
    }

    /// <summary>Writes to <see cref="System.Diagnostics.Trace"/>, the sink Bq itself uses.</summary>
    public class DefaultFactory : LoggingFactoryDefinition
    {
        LogLevel level = LogLevel.Info;

        public void Level(LogLevel level) => this.level = level;

        protected internal override ILoggerFactory GetLoggingFactory() => new TraceLoggerFactory(level);

        sealed class TraceLoggerFactory : ILoggerFactory
        {
            readonly LogLevel level;
            public TraceLoggerFactory(LogLevel level) { this.level = level; }
            public ILog GetLogger(Type type) => GetLogger((type ?? throw new ArgumentNullException(nameof(type))).FullName);
            public ILog GetLogger(string name)
            {
                if (string.IsNullOrEmpty(name)) throw new ArgumentException("A logger name is required.", nameof(name));
                return new TraceLog(name, level);
            }
        }

        sealed class TraceLog : ILog
        {
            readonly string name;
            readonly LogLevel minimum;
            public TraceLog(string name, LogLevel minimum) { this.name = name; this.minimum = minimum; }
            bool Enabled(LogLevel value) => value >= minimum;
            public bool IsDebugEnabled => Enabled(LogLevel.Debug);
            public bool IsInfoEnabled => Enabled(LogLevel.Info);
            public bool IsWarnEnabled => Enabled(LogLevel.Warn);
            public bool IsErrorEnabled => Enabled(LogLevel.Error);
            public bool IsFatalEnabled => Enabled(LogLevel.Fatal);

            void Write(LogLevel value, string message, Exception exception)
            {
                if (!Enabled(value)) return;
                var text = name + ": " + message + (exception == null ? "" : Environment.NewLine + exception);
                if (value >= LogLevel.Error) System.Diagnostics.Trace.TraceError(text);
                else if (value == LogLevel.Warn) System.Diagnostics.Trace.TraceWarning(text);
                else System.Diagnostics.Trace.TraceInformation(text);
            }

            static string Format(string format, object[] args) => string.Format(format, args);
            public void Debug(string message) => Write(LogLevel.Debug, message, null);
            public void Debug(string message, Exception exception) => Write(LogLevel.Debug, message, exception);
            public void DebugFormat(string format, params object[] args) { if (IsDebugEnabled) Write(LogLevel.Debug, Format(format, args), null); }
            public void Info(string message) => Write(LogLevel.Info, message, null);
            public void Info(string message, Exception exception) => Write(LogLevel.Info, message, exception);
            public void InfoFormat(string format, params object[] args) { if (IsInfoEnabled) Write(LogLevel.Info, Format(format, args), null); }
            public void Warn(string message) => Write(LogLevel.Warn, message, null);
            public void Warn(string message, Exception exception) => Write(LogLevel.Warn, message, exception);
            public void WarnFormat(string format, params object[] args) { if (IsWarnEnabled) Write(LogLevel.Warn, Format(format, args), null); }
            public void Error(string message) => Write(LogLevel.Error, message, null);
            public void Error(string message, Exception exception) => Write(LogLevel.Error, message, exception);
            public void ErrorFormat(string format, params object[] args) { if (IsErrorEnabled) Write(LogLevel.Error, Format(format, args), null); }
            public void Fatal(string message) => Write(LogLevel.Fatal, message, null);
            public void Fatal(string message, Exception exception) => Write(LogLevel.Fatal, message, exception);
            public void FatalFormat(string format, params object[] args) { if (IsFatalEnabled) Write(LogLevel.Fatal, Format(format, args), null); }
        }
    }

    public static class LogManager
    {
        static Lazy<ILoggerFactory> loggerFactory;

        static LogManager() { Use<DefaultFactory>(); }

        public static T Use<T>() where T : LoggingFactoryDefinition, new()
        {
            var definition = new T();
            loggerFactory = new Lazy<ILoggerFactory>(definition.GetLoggingFactory);
            return definition;
        }

        public static void UseFactory(ILoggerFactory loggerFactory)
        {
            if (loggerFactory == null) throw new ArgumentNullException(nameof(loggerFactory));
            LogManager.loggerFactory = new Lazy<ILoggerFactory>(() => loggerFactory);
        }

        public static ILog GetLogger<T>() => GetLogger(typeof(T));
        public static ILog GetLogger(Type type) => loggerFactory.Value.GetLogger(type ?? throw new ArgumentNullException(nameof(type)));
        public static ILog GetLogger(string name) => loggerFactory.Value.GetLogger(name);
    }
}
