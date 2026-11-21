using System;
using System.Collections.Generic;

namespace Bq.Messaging
{
    public interface IExtendable
    {
        ContextBag Extensions { get; }
    }

    public abstract class ExtendableOptions
    {
        internal ContextBag Extensions { get; } = new ContextBag();
        internal bool ImmediateDispatch { get; set; }
        internal string MessageId { get; set; }
        internal IDictionary<string, object> Headers { get; } = new Dictionary<string, object>();
    }

    public static class ImmediateDispatchOptionExtensions
    {
        public static void RequireImmediateDispatch(this ExtendableOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (options is MessageOptions message) message.RequireImmediateDispatch();
            else options.ImmediateDispatch = true;
        }

        public static bool RequiredImmediateDispatch(this ExtendableOptions options) =>
            (options ?? throw new ArgumentNullException(nameof(options))).ImmediateDispatch;
    }

    public static class ExtendableOptionsExtensions
    {
        public static ContextBag GetExtensions(this ExtendableOptions options) =>
            (options ?? throw new ArgumentNullException(nameof(options))).Extensions;
    }

    public interface ReadOnlyContextBag
    {
        T Get<T>();
        T Get<T>(string key);
        bool TryGet<T>(out T result);
        bool TryGet<T>(string key, out T result);
    }

    public class ContextBag : ReadOnlyContextBag
    {
        readonly ContextBag parent;
        readonly Dictionary<string, object> values = new Dictionary<string, object>(StringComparer.Ordinal);

        public ContextBag(ContextBag parentBag = null) { parent = parentBag; }

        public T Get<T>() => Get<T>(TypeKey<T>());

        public T Get<T>(string key)
        {
            if (TryGet<T>(key, out var value)) return value;
            throw new KeyNotFoundException("No extension is registered for " + key);
        }

        public bool TryGet<T>(out T result) => TryGet(TypeKey<T>(), out result);

        public bool TryGet<T>(string key, out T result)
        {
            ValidateKey(key);
            if (values.TryGetValue(key, out var existing))
            {
                result = (T)existing;
                return true;
            }
            if (parent != null) return parent.TryGet(key, out result);
            result = default;
            return false;
        }

        public T GetOrCreate<T>() where T : class, new()
        {
            if (TryGet<T>(out var value)) return value;
            value = new T();
            Set(value);
            return value;
        }

        public void Set<T>(T t) => Set(TypeKey<T>(), t);

        public void Set<T>(string key, T t)
        {
            ValidateKey(key);
            values[key] = t;
        }

        public void Remove<T>() => Remove(TypeKey<T>());
        public void Remove(string key)
        {
            ValidateKey(key);
            values.Remove(key);
        }

        static void ValidateKey(string key)
        {
            if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));
        }

        static string TypeKey<T>() => typeof(T).FullName;
    }
}
