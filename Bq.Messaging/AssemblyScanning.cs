using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Bq.Messaging.ObjectBuilder;

namespace Bq.Messaging
{
    public class AssemblyScannerConfiguration
    {
        internal readonly List<string> ExcludedAssemblies = new List<string>();
        internal readonly List<Type> ExcludedTypes = new List<Type>();

        public bool ScanAppDomainAssemblies { get; set; } = true;
        public bool ScanFileSystemAssemblies { get; set; } = true;
        public bool ScanAssembliesInNestedDirectories { get; set; }
        public bool ThrowExceptions { get; set; } = true;
        public string AdditionalAssemblyScanningPath { get; set; }

        public void ExcludeAssemblies(params string[] assemblies) =>
            ExcludedAssemblies.AddRange(assemblies ?? throw new ArgumentNullException(nameof(assemblies)));

        public void ExcludeTypes(params Type[] types) =>
            ExcludedTypes.AddRange(types ?? throw new ArgumentNullException(nameof(types)));
    }

    public static class AssemblyScannerConfigurationExtensions
    {
        public static AssemblyScannerConfiguration AssemblyScanner(this EndpointConfiguration endpointConfiguration) =>
            (endpointConfiguration ?? throw new ArgumentNullException(nameof(endpointConfiguration))).Scanner;
    }

    public sealed partial class EndpointConfiguration
    {
        internal AssemblyScannerConfiguration Scanner { get; } = new AssemblyScannerConfiguration();

        public void ExcludeAssemblies(params string[] assemblies) => Scanner.ExcludeAssemblies(assemblies);
        public void ExcludeTypes(params Type[] types) => Scanner.ExcludeTypes(types);
        public void ScanAssembliesInNestedDirectories() => Scanner.ScanAssembliesInNestedDirectories = true;

        static readonly string[] FrameworkPrefixes =
            { "System", "Microsoft", "mscorlib", "netstandard", "Mono.", "Newtonsoft", "RabbitMQ.Client", "MessagePack", "Castle." };

        /// <summary>
        /// Registers handlers and sagas found in scanned assemblies. Message types that already have an explicit
        /// handler, and sagas that are already registered, are left alone.
        /// </summary>
        internal void ApplyAssemblyScanning()
        {
            var excluded = new HashSet<string>(Scanner.ExcludedAssemblies.Select(AssemblyName), StringComparer.OrdinalIgnoreCase);
            var types = ScannedAssemblies(excluded).SelectMany(TypesOf)
                .Where(type => type.IsClass && !type.IsAbstract && !type.IsGenericTypeDefinition && !Scanner.ExcludedTypes.Contains(type))
                .Distinct().ToList();

            // ExecuteTheseHandlersFirst: listed handlers register first and so run first; the rest keep scan order.
            types = types.Select((type, index) => (type, index))
                .OrderBy(entry => HandlerOrder.IndexOf(entry.type) is var position && position >= 0 ? position : int.MaxValue)
                .ThenBy(entry => entry.index).Select(entry => entry.type).ToList();
            Settings.Set<IList<Type>>(types);
            var handlerRegistrations = new List<Action>();
            var componentRegistrations = new List<Type>();
            foreach (var type in types)
            {
                if (typeof(Bq.Messaging.Features.Feature).IsAssignableFrom(type))
                {
                    if (type.GetConstructor(Type.EmptyTypes) != null) RegisterFeatureType(type);
                    continue;
                }
                var sagaData = SagaDataType(type);
                if (sagaData != null)
                {
                    if (SagaTimeouts.ContainsKey(MessageType(type))) continue;
                    componentRegistrations.Add(type);
                    handlerRegistrations.Add(() => InvokeScan(nameof(RegisterScannedSaga), new[] { type, sagaData }));
                    continue;
                }
                var handled = type.GetInterfaces().Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IHandleMessages<>))
                    .Select(i => i.GetGenericArguments()[0]).Where(message => !message.IsGenericParameter).ToList();
                if (handled.Count == 0) continue;
                componentRegistrations.Add(type);
                foreach (var message in handled)
                {
                    if (Handlers.ContainsKey(MessageType(message))) continue;
                    handlerRegistrations.Add(() => InvokeScan(nameof(RegisterScannedHandler), new[] { message }, type));
                }
            }
            if (componentRegistrations.Count == 0) return;
            // Scanned registrations come first so explicit RegisterComponents calls can override them.
            this.componentRegistrations.Insert(0, components =>
            {
                foreach (var component in componentRegistrations)
                    if (!components.HasComponent(component)) components.ConfigureComponent(component, DependencyLifecycle.InstancePerUnitOfWork);
            });
            foreach (var register in handlerRegistrations) register();
        }

        static Type SagaDataType(Type type)
        {
            for (var current = type.BaseType; current != null; current = current.BaseType)
                if (current.IsGenericType && current.GetGenericTypeDefinition() == typeof(Saga<>)) return current.GetGenericArguments()[0];
            return null;
        }

        void InvokeScan(string method, Type[] genericArguments, params object[] arguments) =>
            typeof(EndpointConfiguration).GetMethod(method, BindingFlags.Instance | BindingFlags.NonPublic)
                .MakeGenericMethod(genericArguments).Invoke(this, arguments);

        void RegisterScannedHandler<TMessage>(Type handlerType) =>
            Handle<TMessage>(context => (IHandleMessages<TMessage>)RabbitEndpoint.BuilderFor(context).Build(handlerType));

        void RegisterScannedSaga<TSaga, TData>() where TSaga : Saga<TData> where TData : class, new() =>
            this.RegisterSaga<TSaga, TData>(
                () => (TSaga)RabbitEndpoint.CurrentBuilder().Build(typeof(TSaga)),
                // Mapping definitions never run saga logic, so they do not need the saga's dependencies.
                () => (TSaga)System.Runtime.Serialization.FormatterServices.GetUninitializedObject(typeof(TSaga)),
                saga => { });

        internal IEnumerable<Assembly> ScannedAssemblies(HashSet<string> excluded)
        {
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            bool Include(string name) => !string.IsNullOrEmpty(name) && !excluded.Contains(name) && seen.Add(name) &&
                !FrameworkPrefixes.Any(prefix => name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase));
            var self = typeof(EndpointConfiguration).Assembly;
            seen.Add(self.GetName().Name);
            if (Scanner.ScanAppDomainAssemblies)
                foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
                    if (!assembly.IsDynamic && Include(assembly.GetName().Name)) yield return assembly;
            if (!Scanner.ScanFileSystemAssemblies) yield break;
            var directories = new List<string> { AppDomain.CurrentDomain.BaseDirectory };
            if (!string.IsNullOrEmpty(Scanner.AdditionalAssemblyScanningPath)) directories.Add(Scanner.AdditionalAssemblyScanningPath);
            var option = Scanner.ScanAssembliesInNestedDirectories ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;
            foreach (var directory in directories.Where(Directory.Exists))
                foreach (var file in Directory.GetFiles(directory, "*.dll", option))
                {
                    if (!Include(AssemblyName(Path.GetFileName(file)))) continue;
                    Assembly loaded = null;
                    try { loaded = Assembly.LoadFrom(file); }
                    catch (Exception) when (!Scanner.ThrowExceptions) { }
                    catch (BadImageFormatException) { }   // native or unrelated binaries
                    catch (FileLoadException) { }
                    if (loaded != null) yield return loaded;
                }
        }

        // Only .dll and .exe are file extensions; "Company.Product.Tests" is an assembly name with dots.
        static string AssemblyName(string nameOrFile) =>
            nameOrFile.EndsWith(".dll", StringComparison.OrdinalIgnoreCase) || nameOrFile.EndsWith(".exe", StringComparison.OrdinalIgnoreCase)
                ? nameOrFile.Substring(0, nameOrFile.Length - 4) : nameOrFile;

        static IEnumerable<Type> TypesOf(Assembly assembly)
        {
            try { return assembly.GetTypes(); }
            catch (ReflectionTypeLoadException ex) { return ex.Types.Where(type => type != null); }
        }
    }
}
