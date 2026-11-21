using System;
using System.Collections.Generic;

namespace Bq.Messaging.Settings
{
    public interface ReadOnlySettings
    {
        T Get<T>(string key);
        T Get<T>();
        object Get(string key);
        bool TryGet<T>(out T value);
        bool TryGet<T>(string key, out T value);
        T GetOrDefault<T>();
        T GetOrDefault<T>(string key);
        bool HasSetting(string key);
        bool HasSetting<T>();
        bool HasExplicitValue(string key);
        bool HasExplicitValue<T>();
    }

    public class SettingsHolder : ReadOnlySettings
    {
        readonly Dictionary<string, object> overrides = new Dictionary<string, object>(StringComparer.Ordinal);
        readonly Dictionary<string, object> defaults = new Dictionary<string, object>(StringComparer.Ordinal);
        readonly object gate = new object();

        static string Require(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentException("A settings key is required.", nameof(key));
            return key;
        }

        public T Get<T>(string key) => (T)Get(key);
        public T Get<T>() => (T)Get(typeof(T).FullName);

        public object Get(string key)
        {
            lock (gate)
            {
                if (overrides.TryGetValue(Require(key), out var value) || defaults.TryGetValue(key, out value)) return value;
            }
            throw new KeyNotFoundException("The given key (" + key + ") was not present in the dictionary.");
        }

        public bool TryGet<T>(out T value) => TryGet(typeof(T).FullName, out value);

        public bool TryGet<T>(string key, out T value)
        {
            lock (gate)
            {
                if ((overrides.TryGetValue(Require(key), out var found) || defaults.TryGetValue(key, out found)) && found is T typed)
                {
                    value = typed;
                    return true;
                }
            }
            value = default(T);
            return false;
        }

        public T GetOrDefault<T>() => GetOrDefault<T>(typeof(T).FullName);

        public T GetOrDefault<T>(string key)
        {
            lock (gate)
            {
                if (overrides.TryGetValue(Require(key), out var value) || defaults.TryGetValue(key, out value)) return (T)value;
            }
            return default(T);
        }

        public T GetOrCreate<T>() where T : class, new()
        {
            lock (gate)
            {
                var key = typeof(T).FullName;
                if ((overrides.TryGetValue(key, out var found) || defaults.TryGetValue(key, out found)) && found is T existing) return existing;
                var created = new T();
                overrides[key] = created;
                return created;
            }
        }

        public bool HasSetting(string key) { lock (gate) return overrides.ContainsKey(Require(key)) || defaults.ContainsKey(key); }
        public bool HasSetting<T>() => HasSetting(typeof(T).FullName);
        public bool HasExplicitValue(string key) { lock (gate) return overrides.ContainsKey(Require(key)); }
        public bool HasExplicitValue<T>() => HasExplicitValue(typeof(T).FullName);

        public void Set(string key, object value) { lock (gate) overrides[Require(key)] = value; }
        public void Set<T>(T value) => Set(typeof(T).FullName, value);
        public void Set<T>(Action value) => Set(typeof(T).FullName, value);
        public void Set<T>(object value) => Set(typeof(T).FullName, value);
        public void SetDefault<T>(object value) => SetDefault(typeof(T).FullName, value);
        public void SetDefault(string key, object value) { lock (gate) defaults[Require(key)] = value; }
        public void SetDefault<T>(T value) => SetDefault(typeof(T).FullName, value);
        public void SetDefault<T>(Action value) => SetDefault(typeof(T).FullName, value);

        public void Clear() { lock (gate) { overrides.Clear(); defaults.Clear(); } }
    }
}

namespace Bq.Messaging.Configuration.AdvancedExtensibility
{
    using Bq.Messaging.Settings;

    public abstract class ExposeSettings
    {
        protected ExposeSettings(SettingsHolder settings) { Settings = settings ?? throw new ArgumentNullException(nameof(settings)); }
        internal SettingsHolder Settings { get; }
    }

    public static class AdvancedExtensibilityExtensions
    {
        public static SettingsHolder GetSettings(this ExposeSettings exposeSettings) =>
            (exposeSettings ?? throw new ArgumentNullException(nameof(exposeSettings))).Settings;
    }
}
