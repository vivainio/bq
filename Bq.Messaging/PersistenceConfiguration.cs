using System;
using System.Threading.Tasks;
using Bq.Messaging.Configuration.AdvancedExtensibility;
using Bq.Messaging.Settings;

namespace Bq.Messaging
{
    /// <summary>A persistence provider that can create its own schema when installers are enabled.</summary>
    public interface IInstallablePersistence
    {
        Task Initialize();
    }

    /// <summary>
    /// Describes a persistence technology for <c>UsePersistence</c>. Bq uses one provider for the inbox, outbox,
    /// sagas, timeouts and recovery records, so a definition supplies that provider.
    /// </summary>
    public abstract class PersistenceDefinition
    {
        readonly System.Collections.Generic.List<Type> supported = new System.Collections.Generic.List<Type>();
        readonly System.Collections.Generic.List<Action<SettingsHolder>> defaults = new System.Collections.Generic.List<Action<SettingsHolder>>();

        public abstract IPersistenceProvider CreateProvider(ReadOnlySettings settings);

        protected void Supports<T>(Action<SettingsHolder> action) where T : StorageType
        {
            supported.Add(typeof(T));
            if (action != null) defaults.Add(action);
        }

        protected void Defaults(Action<SettingsHolder> action) => defaults.Add(action ?? throw new ArgumentNullException(nameof(action)));
        public bool HasSupportFor<T>() where T : StorageType => HasSupportFor(typeof(T));
        public bool HasSupportFor(Type storageType) => supported.Contains(storageType ?? throw new ArgumentNullException(nameof(storageType)));
        internal void ApplyDefaults(SettingsHolder settings) { foreach (var action in defaults) action(settings); }
    }

    /// <summary>Storage categories used by <c>UsePersistence&lt;T, S&gt;</c>. Bq stores them all in one provider.</summary>
    public abstract class StorageType
    {
        public sealed class Sagas : StorageType { }
        public sealed class Timeouts : StorageType { }
        public sealed class Outbox : StorageType { }
        public sealed class Subscriptions : StorageType { }
        public sealed class GatewayDeduplication : StorageType { }
    }

    public class PersistenceExtensions : ExposeSettings
    {
        public PersistenceExtensions(SettingsHolder settings) : base(settings) { }
        public PersistenceExtensions(Type definitionType, SettingsHolder settings, Type storageType) : base(settings) { }
    }

    public class PersistenceExtensions<T> : PersistenceExtensions where T : PersistenceDefinition
    {
        public PersistenceExtensions(SettingsHolder settings) : base(settings) { }
        public PersistenceExtensions(SettingsHolder settings, Type storageType) : base(settings) { }
    }

    public class PersistenceExtensions<T, S> : PersistenceExtensions<T> where T : PersistenceDefinition where S : StorageType
    {
        public PersistenceExtensions(SettingsHolder settings) : base(settings, typeof(S)) { }
    }

    public static class PersistenceConfig
    {
        internal const string DefinitionKey = "Bq.Persistence.Definition";

        public static PersistenceExtensions<T> UsePersistence<T>(this EndpointConfiguration configuration) where T : PersistenceDefinition, new()
        {
            Select(configuration, typeof(T));
            return new PersistenceExtensions<T>(configuration.GetSettings());
        }

        public static PersistenceExtensions<T, S> UsePersistence<T, S>(this EndpointConfiguration configuration)
            where T : PersistenceDefinition, new() where S : StorageType
        {
            Select(configuration, typeof(T));
            return new PersistenceExtensions<T, S>(configuration.GetSettings());
        }

        public static PersistenceExtensions UsePersistence(this EndpointConfiguration configuration, Type definitionType)
        {
            Select(configuration, definitionType);
            return new PersistenceExtensions(configuration.GetSettings());
        }

        static void Select(EndpointConfiguration configuration, Type definitionType)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (definitionType == null || definitionType.IsAbstract || !typeof(PersistenceDefinition).IsAssignableFrom(definitionType))
                throw new ArgumentException("A concrete PersistenceDefinition type is required.", nameof(definitionType));
            var settings = configuration.GetSettings();
            if (settings.TryGet<Type>(DefinitionKey, out var existing) && existing != definitionType)
                throw new InvalidOperationException("Bq stores all messaging data in one persistence provider. " + existing.Name + " is already selected.");
            settings.Set(DefinitionKey, definitionType);
        }
    }

    public sealed partial class EndpointConfiguration
    {
        bool installersEnabled;

        /// <summary>Creates the persistence schema when the endpoint starts, if the provider supports it. The username is not used.</summary>
        public void EnableInstallers(string username = null) => installersEnabled = true;

        /// <summary>Creates the provider selected by <c>UsePersistence</c> and runs installers. Called when the endpoint starts.</summary>
        internal void ApplyPersistenceSettings()
        {
            if (Persistence == null && Settings.TryGet<Type>(PersistenceConfig.DefinitionKey, out var definitionType))
            {
                var definition = (PersistenceDefinition)Activator.CreateInstance(definitionType);
                definition.ApplyDefaults(Settings);
                Persistence = definition.CreateProvider(Settings);
            }
            if (installersEnabled && Persistence is IInstallablePersistence installable)
                installable.Initialize().GetAwaiter().GetResult();
        }
    }
}
