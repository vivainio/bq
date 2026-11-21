using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Bq.Messaging.ObjectBuilder;
using Bq.Messaging.Settings;
using Bq.Messaging.Transport;

namespace Bq.Messaging.Features
{
    public enum FeatureState { Disabled, Enabled, Deactivated, Active }

    public abstract class FeatureStartupTask
    {
        protected abstract Task OnStart(IMessageSession session);
        protected virtual Task OnStop(IMessageSession session) => Task.CompletedTask;
        internal Task PerformStartup(IMessageSession session) => OnStart(session);
        internal Task PerformStop(IMessageSession session) => OnStop(session);
    }

    public sealed class FeatureConfigurationContext
    {
        internal FeatureConfigurationContext(EndpointConfiguration configuration, DeferredComponents components)
        {
            Settings = configuration.GetSettingsHolder();
            Pipeline = configuration.Pipeline;
            Container = components;
            StartupTasks = configuration.FeatureStartupTasks;
            satellites = configuration.Satellites;
        }

        readonly List<Func<IBuilder, FeatureStartupTask>> StartupTasks;
        readonly List<SatelliteReceiver> satellites;

        public ReadOnlySettings Settings { get; }
        public IConfigureComponents Container { get; }
        public PipelineSettings Pipeline { get; }

        public void AddSatelliteReceiver(string name, string transportAddress, PushRuntimeSettings runtimeSettings,
            Func<RecoverabilityConfig, ErrorContext, RecoverabilityAction> recoverabilityPolicy, Func<IBuilder, MessageContext, Task> onMessage) =>
            AddSatelliteReceiver(name, transportAddress, TransportTransactionMode.ReceiveOnly, runtimeSettings, recoverabilityPolicy, onMessage);

        /// <summary>
        /// Adds an extra input queue with its own processing delegate. Delayed retries hold the delivery unacknowledged
        /// for the delay and then requeue it; failed messages move to the error queue with failure headers.
        /// </summary>
        public void AddSatelliteReceiver(string name, string transportAddress, TransportTransactionMode requiredTransportTransactionMode,
            PushRuntimeSettings runtimeSettings, Func<RecoverabilityConfig, ErrorContext, RecoverabilityAction> recoverabilityPolicy,
            Func<IBuilder, MessageContext, Task> onMessage)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("A satellite name is required.", nameof(name));
            if (string.IsNullOrWhiteSpace(transportAddress)) throw new ArgumentException("A satellite address is required.", nameof(transportAddress));
            if (requiredTransportTransactionMode != TransportTransactionMode.ReceiveOnly && requiredTransportTransactionMode != TransportTransactionMode.None)
                throw new NotSupportedException("Satellite receivers support only ReceiveOnly processing.");
            if (satellites.Exists(s => s.Address == transportAddress)) throw new InvalidOperationException("A satellite already reads " + transportAddress);
            satellites.Add(new SatelliteReceiver(name, transportAddress, runtimeSettings ?? PushRuntimeSettings.Default,
                recoverabilityPolicy ?? throw new ArgumentNullException(nameof(recoverabilityPolicy)),
                onMessage ?? throw new ArgumentNullException(nameof(onMessage))));
        }

        public void RegisterStartupTask<T>(Func<IBuilder, T> startupTaskFactory) where T : FeatureStartupTask
        {
            if (startupTaskFactory == null) throw new ArgumentNullException(nameof(startupTaskFactory));
            StartupTasks.Add(builder => startupTaskFactory(builder));
        }

        public void RegisterStartupTask<T>(Func<T> startupTaskFactory) where T : FeatureStartupTask
        {
            if (startupTaskFactory == null) throw new ArgumentNullException(nameof(startupTaskFactory));
            StartupTasks.Add(_ => startupTaskFactory());
        }

        public void RegisterStartupTask<TTask>(TTask startupTask) where TTask : FeatureStartupTask
        {
            if (startupTask == null) throw new ArgumentNullException(nameof(startupTask));
            StartupTasks.Add(_ => startupTask);
        }
    }

    /// <summary>Collects component registrations made by features until the endpoint container exists.</summary>
    sealed class DeferredComponents : IConfigureComponents
    {
        readonly List<Action<IConfigureComponents>> actions;
        readonly HashSet<Type> known = new HashSet<Type>();

        public DeferredComponents(List<Action<IConfigureComponents>> actions) { this.actions = actions; }

        void Track(Type type) { foreach (var service in new[] { type }.Concat(type.GetInterfaces())) known.Add(service); }

        public void ConfigureComponent(Type concreteComponent, DependencyLifecycle dependencyLifecycle)
        {
            Track(concreteComponent);
            actions.Add(components => components.ConfigureComponent(concreteComponent, dependencyLifecycle));
        }

        public void ConfigureComponent<T>(DependencyLifecycle dependencyLifecycle) => ConfigureComponent(typeof(T), dependencyLifecycle);

        public void ConfigureComponent<T>(Func<T> componentFactory, DependencyLifecycle dependencyLifecycle)
        {
            Track(typeof(T));
            actions.Add(components => components.ConfigureComponent(componentFactory, dependencyLifecycle));
        }

        public void ConfigureComponent<T>(Func<IBuilder, T> componentFactory, DependencyLifecycle dependencyLifecycle)
        {
            Track(typeof(T));
            actions.Add(components => components.ConfigureComponent(componentFactory, dependencyLifecycle));
        }

        public bool HasComponent<T>() => known.Contains(typeof(T));
        public bool HasComponent(Type componentType) => known.Contains(componentType);

        public void RegisterSingleton(Type lookupType, object instance)
        {
            Track(lookupType);
            actions.Add(components => components.RegisterSingleton(lookupType, instance));
        }

        public void RegisterSingleton<T>(T instance) => RegisterSingleton(typeof(T), instance);
    }

    public abstract class Feature
    {
        readonly List<Action<SettingsHolder>> defaults = new List<Action<SettingsHolder>>();
        readonly List<(Func<FeatureConfigurationContext, bool> Check, string Description)> prerequisites =
            new List<(Func<FeatureConfigurationContext, bool>, string)>();
        internal readonly List<object> Dependencies = new List<object>();
        internal readonly List<object[]> AnyOfDependencies = new List<object[]>();
        internal readonly List<object> OptionalDependencies = new List<object>();

        protected Feature() { Name = GetType().Name; Version = GetType().Assembly.GetName().Version.ToString(3); }

        public bool IsEnabledByDefault { get; private set; }
        public bool IsActive { get; private set; }
        public string Name { get; }
        public string Version { get; }

        protected void EnableByDefault() => IsEnabledByDefault = true;
        protected void DependsOn<T>() where T : Feature => Dependencies.Add(typeof(T));
        protected void DependsOn(string featureName) => Dependencies.Add(featureName ?? throw new ArgumentNullException(nameof(featureName)));
        protected void DependsOnOptionally<T>() where T : Feature => OptionalDependencies.Add(typeof(T));
        protected void DependsOnOptionally(Type featureType) => OptionalDependencies.Add(featureType ?? throw new ArgumentNullException(nameof(featureType)));
        protected void DependsOnOptionally(string featureName) => OptionalDependencies.Add(featureName ?? throw new ArgumentNullException(nameof(featureName)));
        protected void DependsOnAtLeastOne(params Type[] featureTypes) => AnyOfDependencies.Add(featureTypes.Cast<object>().ToArray());
        protected void DependsOnAtLeastOne(params string[] featureNames) => AnyOfDependencies.Add(featureNames.Cast<object>().ToArray());
        protected void Defaults(Action<SettingsHolder> settings) => defaults.Add(settings ?? throw new ArgumentNullException(nameof(settings)));
        protected void Prerequisite(Func<FeatureConfigurationContext, bool> condition, string description) =>
            prerequisites.Add((condition ?? throw new ArgumentNullException(nameof(condition)), description));

        protected abstract void Setup(FeatureConfigurationContext context);

        public override string ToString() => Name + " [" + Version + "]";

        internal void ApplyDefaults(SettingsHolder settings) { foreach (var action in defaults) action(settings); }
        internal bool PrerequisitesMet(FeatureConfigurationContext context) => prerequisites.All(p => p.Check(context));
        internal void Activate(FeatureConfigurationContext context) { Setup(context); IsActive = true; }
    }

    public static class SettingsExtensions
    {
        public static SettingsHolder EnableFeatureByDefault<T>(this SettingsHolder settings) where T : Feature => settings.EnableFeatureByDefault(typeof(T));

        public static SettingsHolder EnableFeatureByDefault(this SettingsHolder settings, Type featureType)
        {
            (settings ?? throw new ArgumentNullException(nameof(settings))).SetDefault(StateKey(featureType), FeatureState.Enabled);
            return settings;
        }

        public static string EndpointName(this ReadOnlySettings settings) => settings.Get<string>(EndpointConfiguration.EndpointNameKey);

        public static string LocalAddress(this ReadOnlySettings settings) =>
            settings.GetOrDefault<string>("Bq.LocalAddress") ?? settings.EndpointName();

        public static string InstanceSpecificQueue(this ReadOnlySettings settings) => settings.GetOrDefault<string>("Bq.InstanceSpecificQueue");

        /// <summary>The types found by assembly scanning, available to features once scanning has run.</summary>
        public static IList<Type> GetAvailableTypes(this ReadOnlySettings settings) =>
            settings.GetOrDefault<IList<Type>>("Bq.AvailableTypes") ?? new List<Type>();

        public static bool IsFeatureEnabled<T>(this ReadOnlySettings settings) where T : Feature => settings.IsFeatureEnabled(typeof(T));
        public static bool IsFeatureEnabled(this ReadOnlySettings settings, Type featureType) =>
            settings.GetOrDefault<FeatureState>(StateKey(featureType)) is FeatureState state && (state == FeatureState.Enabled || state == FeatureState.Active);

        public static bool IsFeatureActive<T>(this ReadOnlySettings settings) where T : Feature => settings.IsFeatureActive(typeof(T));
        public static bool IsFeatureActive(this ReadOnlySettings settings, Type featureType) =>
            settings.GetOrDefault<FeatureState>(StateKey(featureType)) == FeatureState.Active;

        internal static string StateKey(Type featureType) => "Bq.Feature." + (featureType ?? throw new ArgumentNullException(nameof(featureType))).FullName;
    }
}

namespace Bq.Messaging
{
    using Bq.Messaging.Features;

    public static class FeatureEndpointConfigurationExtensions
    {
        public static void EnableFeature<T>(this EndpointConfiguration configuration) where T : Feature, new() =>
            configuration.EnableFeature(typeof(T));

        public static void EnableFeature(this EndpointConfiguration configuration, Type featureType) =>
            (configuration ?? throw new ArgumentNullException(nameof(configuration))).SetFeatureState(featureType, FeatureState.Enabled);

        public static void DisableFeature<T>(this EndpointConfiguration configuration) where T : Feature, new() =>
            configuration.DisableFeature(typeof(T));

        public static void DisableFeature(this EndpointConfiguration configuration, Type featureType) =>
            (configuration ?? throw new ArgumentNullException(nameof(configuration))).SetFeatureState(featureType, FeatureState.Disabled);
    }

    public sealed partial class EndpointConfiguration
    {
        readonly Dictionary<Type, Feature> features = new Dictionary<Type, Feature>();
        internal readonly List<Func<IBuilder, Features.FeatureStartupTask>> FeatureStartupTasks = new List<Func<IBuilder, Features.FeatureStartupTask>>();
        readonly List<Features.FeatureStartupTask> runningTasks = new List<Features.FeatureStartupTask>();
        bool featuresActivated;

        internal Settings.SettingsHolder GetSettingsHolder() => Settings;

        internal void RegisterFeatureType(Type featureType)
        {
            if (featureType == null) throw new ArgumentNullException(nameof(featureType));
            if (!typeof(Feature).IsAssignableFrom(featureType) || featureType.IsAbstract || featureType.GetConstructor(Type.EmptyTypes) == null)
                throw new ArgumentException("A concrete Feature with a public parameterless constructor is required.", nameof(featureType));
            if (!features.ContainsKey(featureType)) features.Add(featureType, (Feature)Activator.CreateInstance(featureType));
        }

        internal void SetFeatureState(Type featureType, FeatureState state)
        {
            RegisterFeatureType(featureType);
            Settings.Set(SettingsExtensions.StateKey(featureType), state);
        }

        FeatureState StateOf(Type featureType) => Settings.GetOrDefault<FeatureState>(SettingsExtensions.StateKey(featureType));

        /// <summary>Applies feature defaults, then activates enabled features once their dependencies are active.</summary>
        internal void ActivateFeatures()
        {
            if (featuresActivated) return;
            featuresActivated = true;
            if (features.Count == 0) return;
            foreach (var feature in features.Values) feature.ApplyDefaults(Settings);
            foreach (var pair in features)
                if (!Settings.HasExplicitValue(SettingsExtensions.StateKey(pair.Key)) && pair.Value.IsEnabledByDefault)
                    Settings.SetDefault(SettingsExtensions.StateKey(pair.Key), FeatureState.Enabled);

            var components = new Features.DeferredComponents(componentRegistrations);
            var context = new FeatureConfigurationContext(this, components);
            var pending = features.Keys.Where(type => StateOf(type) == FeatureState.Enabled).ToList();

            Type Find(object reference) => reference as Type ??
                features.Keys.FirstOrDefault(type => features[type].Name == (string)reference || type.FullName == (string)reference);
            FeatureState Stateof(object reference) { var type = Find(reference); return type == null ? FeatureState.Disabled : StateOf(type); }

            bool progress;
            do
            {
                progress = false;
                foreach (var type in pending.ToList())
                {
                    var feature = features[type];
                    if (feature.Dependencies.Any(dependency => Stateof(dependency) == FeatureState.Disabled || Stateof(dependency) == FeatureState.Deactivated) ||
                        feature.AnyOfDependencies.Any(group => group.All(dependency => Stateof(dependency) == FeatureState.Disabled || Stateof(dependency) == FeatureState.Deactivated)))
                    {
                        Settings.Set(SettingsExtensions.StateKey(type), FeatureState.Deactivated);
                        pending.Remove(type);
                        progress = true;
                        continue;
                    }
                    var ready = feature.Dependencies.All(dependency => Stateof(dependency) == FeatureState.Active) &&
                        feature.AnyOfDependencies.All(group => group.Any(dependency => Stateof(dependency) == FeatureState.Active)) &&
                        feature.OptionalDependencies.All(dependency => Stateof(dependency) != FeatureState.Enabled);
                    if (!ready) continue;
                    pending.Remove(type);
                    progress = true;
                    if (!feature.PrerequisitesMet(context)) { Settings.Set(SettingsExtensions.StateKey(type), FeatureState.Deactivated); continue; }
                    feature.Activate(context);
                    Settings.Set(SettingsExtensions.StateKey(type), FeatureState.Active);
                }
            } while (progress && pending.Count > 0);
            foreach (var type in pending) Settings.Set(SettingsExtensions.StateKey(type), FeatureState.Deactivated);
        }

        internal async Task StartFeatureTasks(IBuilder builder, IMessageSession session)
        {
            foreach (var factory in FeatureStartupTasks)
            {
                var task = factory(builder) ?? throw new InvalidOperationException("A feature startup task factory returned null.");
                await task.PerformStartup(session).ConfigureAwait(false);
                runningTasks.Add(task);
            }
        }

        internal async Task StopFeatureTasks(IMessageSession session)
        {
            Exception failure = null;
            for (var index = runningTasks.Count - 1; index >= 0; index--)
            {
                try { await runningTasks[index].PerformStop(session).ConfigureAwait(false); }
                catch (Exception ex) { failure = failure ?? ex; }
            }
            runningTasks.Clear();
            if (failure != null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
        }
    }
}
