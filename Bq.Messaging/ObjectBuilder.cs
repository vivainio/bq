using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Bq.Messaging.Configuration.AdvancedExtensibility;
using Bq.Messaging.Container;
using Bq.Messaging.ObjectBuilder;
using Bq.Messaging.ObjectBuilder.Common;
using Bq.Messaging.Settings;

namespace Bq.Messaging
{
    public enum DependencyLifecycle
    {
        InstancePerCall,
        SingleInstance,
        InstancePerUnitOfWork
    }
}

namespace Bq.Messaging.ObjectBuilder
{
    public interface IBuilder : IDisposable
    {
        object Build(Type typeToBuild);
        IBuilder CreateChildBuilder();
        T Build<T>();
        IEnumerable<T> BuildAll<T>();
        IEnumerable<object> BuildAll(Type typeToBuild);
        void Release(object instance);
        void BuildAndDispatch(Type typeToBuild, Action<object> action);
    }

    public interface IConfigureComponents
    {
        void ConfigureComponent(Type concreteComponent, DependencyLifecycle dependencyLifecycle);
        void ConfigureComponent<T>(DependencyLifecycle dependencyLifecycle);
        void ConfigureComponent<T>(Func<T> componentFactory, DependencyLifecycle dependencyLifecycle);
        void ConfigureComponent<T>(Func<IBuilder, T> componentFactory, DependencyLifecycle dependencyLifecycle);
        bool HasComponent<T>();
        bool HasComponent(Type componentType);
        void RegisterSingleton(Type lookupType, object instance);
        void RegisterSingleton<T>(T instance);
    }
}

namespace Bq.Messaging.ObjectBuilder.Common
{
    public interface IContainer : IDisposable
    {
        object Build(Type typeToBuild);
        IContainer BuildChildContainer();
        IEnumerable<object> BuildAll(Type typeToBuild);
        void Configure(Type component, DependencyLifecycle dependencyLifecycle);
        void Configure<T>(Func<T> component, DependencyLifecycle dependencyLifecycle);
        bool HasComponent(Type componentType);
        void RegisterSingleton(Type lookupType, object instance);
        void Release(object instance);
    }
}

namespace Bq.Messaging.Container
{
    public abstract class ContainerDefinition
    {
        public abstract IContainer CreateContainer(ReadOnlySettings settings);
    }

    public class ContainerCustomizations
    {
        internal ContainerCustomizations(SettingsHolder settings) { Settings = settings; }
        public SettingsHolder Settings { get; }
    }

    /// <summary>Bq's container used when the host does not supply one. Constructor injection, no property injection.</summary>
    public sealed class DefaultContainerDefinition : ContainerDefinition
    {
        public override IContainer CreateContainer(ReadOnlySettings settings) => new DefaultContainer();
    }

    public sealed class DefaultContainer : IContainer
    {
        sealed class Registration
        {
            public Type Implementation;
            public Func<IContainer, object> Factory;
            public DependencyLifecycle Lifecycle;
            public object Singleton;
            public readonly object Gate = new object();
        }

        readonly DefaultContainer parent;
        readonly Dictionary<Type, List<Registration>> registrations;
        readonly object gate;
        readonly Dictionary<Registration, object> scoped = new Dictionary<Registration, object>();
        readonly List<IDisposable> owned = new List<IDisposable>();
        bool disposed;

        public DefaultContainer()
        {
            registrations = new Dictionary<Type, List<Registration>>();
            gate = new object();
        }

        DefaultContainer(DefaultContainer parent)
        {
            this.parent = parent;
            registrations = parent.registrations;
            gate = parent.gate;
        }

        DefaultContainer Root => parent?.Root ?? this;

        static IEnumerable<Type> ServiceTypes(Type implementation) =>
            new[] { implementation }.Concat(implementation.GetInterfaces()).Distinct();

        void Add(IEnumerable<Type> services, Registration registration)
        {
            lock (gate)
            {
                foreach (var service in services)
                {
                    if (!registrations.TryGetValue(service, out var list)) registrations.Add(service, list = new List<Registration>());
                    list.Add(registration);
                }
            }
        }

        public void Configure(Type component, DependencyLifecycle dependencyLifecycle)
        {
            if (component == null) throw new ArgumentNullException(nameof(component));
            if (component.IsAbstract || component.IsInterface) throw new ArgumentException("A concrete component type is required.", nameof(component));
            Add(ServiceTypes(component), new Registration { Implementation = component, Lifecycle = dependencyLifecycle });
        }

        public void Configure<T>(Func<T> component, DependencyLifecycle dependencyLifecycle)
        {
            if (component == null) throw new ArgumentNullException(nameof(component));
            Add(ServiceTypes(typeof(T)), new Registration { Factory = _ => component(), Lifecycle = dependencyLifecycle });
        }

        internal void Configure<T>(Func<IContainer, T> component, DependencyLifecycle dependencyLifecycle)
        {
            if (component == null) throw new ArgumentNullException(nameof(component));
            Add(ServiceTypes(typeof(T)), new Registration { Factory = container => component(container), Lifecycle = dependencyLifecycle });
        }

        public void RegisterSingleton(Type lookupType, object instance)
        {
            if (lookupType == null) throw new ArgumentNullException(nameof(lookupType));
            if (instance == null) throw new ArgumentNullException(nameof(instance));
            Add(new[] { lookupType }.Concat(instance.GetType().GetInterfaces()).Distinct(),
                new Registration { Singleton = instance, Lifecycle = DependencyLifecycle.SingleInstance, Factory = _ => instance });
        }

        public bool HasComponent(Type componentType)
        {
            lock (gate) return registrations.ContainsKey(componentType ?? throw new ArgumentNullException(nameof(componentType)));
        }

        public object Build(Type typeToBuild)
        {
            var found = TryBuild(typeToBuild, out var instance);
            if (!found) throw new InvalidOperationException("No component is registered for " + typeToBuild?.FullName + ".");
            return instance;
        }

        public IEnumerable<object> BuildAll(Type typeToBuild)
        {
            if (typeToBuild == null) throw new ArgumentNullException(nameof(typeToBuild));
            Registration[] list;
            lock (gate) list = registrations.TryGetValue(typeToBuild, out var found) ? found.ToArray() : new Registration[0];
            return list.Select(Resolve).ToList();
        }

        public IContainer BuildChildContainer() => new DefaultContainer(this);

        public void Release(object instance) { }

        bool TryBuild(Type type, out object instance)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));
            if (disposed) throw new ObjectDisposedException(nameof(DefaultContainer));
            if (type == typeof(IContainer) || type == typeof(DefaultContainer)) { instance = this; return true; }
            if (type == typeof(IBuilder)) { instance = new ContainerBuilder(this); return true; }
            Registration registration = null;
            lock (gate)
                if (registrations.TryGetValue(type, out var list)) registration = list[list.Count - 1];
            if (registration == null) { instance = null; return false; }
            instance = Resolve(registration);
            return true;
        }

        object Resolve(Registration registration)
        {
            switch (registration.Lifecycle)
            {
                case DependencyLifecycle.SingleInstance:
                    lock (registration.Gate)
                        return registration.Singleton ?? (registration.Singleton = Root.Create(registration, track: true));
                case DependencyLifecycle.InstancePerUnitOfWork:
                    lock (scoped)
                    {
                        if (!scoped.TryGetValue(registration, out var value))
                            scoped.Add(registration, value = Create(registration, track: true));
                        return value;
                    }
                default:
                    return Create(registration, track: true);
            }
        }

        object Create(Registration registration, bool track)
        {
            var instance = registration.Factory != null ? registration.Factory(this) : Construct(registration.Implementation);
            if (track && instance is IDisposable disposable && registration.Lifecycle != DependencyLifecycle.SingleInstance)
                lock (owned) owned.Add(disposable);
            else if (track && instance is IDisposable single) lock (owned) owned.Add(single);
            return instance;
        }

        object Construct(Type type)
        {
            var constructors = type.GetConstructors().OrderByDescending(c => c.GetParameters().Length).ToArray();
            if (constructors.Length == 0) throw new InvalidOperationException(type.FullName + " has no public constructor.");
            foreach (var constructor in constructors)
            {
                var parameters = constructor.GetParameters();
                var arguments = new object[parameters.Length];
                var satisfied = true;
                for (var index = 0; index < parameters.Length; index++)
                {
                    if (!TryBuild(parameters[index].ParameterType, out arguments[index]))
                    {
                        if (parameters[index].IsOptional) arguments[index] = parameters[index].DefaultValue;
                        else { satisfied = false; break; }
                    }
                }
                if (satisfied) return constructor.Invoke(arguments);
            }
            throw new InvalidOperationException("No constructor of " + type.FullName + " has registered dependencies.");
        }

        public void Dispose()
        {
            if (disposed) return;
            disposed = true;
            List<IDisposable> toDispose;
            lock (owned) { toDispose = new List<IDisposable>(owned); owned.Clear(); }
            toDispose.Reverse();
            foreach (var item in toDispose) item.Dispose();
        }
    }

    /// <summary>Adapts an <see cref="IContainer"/> to the <see cref="IBuilder"/> and <see cref="IConfigureComponents"/> APIs.</summary>
    public sealed class ContainerBuilder : IBuilder, IConfigureComponents
    {
        readonly IContainer container;
        readonly bool ownsContainer;

        public ContainerBuilder(IContainer container) : this(container, false) { }

        ContainerBuilder(IContainer container, bool ownsContainer)
        {
            this.container = container ?? throw new ArgumentNullException(nameof(container));
            this.ownsContainer = ownsContainer;
        }

        public object Build(Type typeToBuild) => container.Build(typeToBuild);
        public T Build<T>() => (T)container.Build(typeof(T));
        public IEnumerable<T> BuildAll<T>() => container.BuildAll(typeof(T)).Cast<T>();
        public IEnumerable<object> BuildAll(Type typeToBuild) => container.BuildAll(typeToBuild);
        public IBuilder CreateChildBuilder() => new ContainerBuilder(container.BuildChildContainer(), true);
        public void Release(object instance) => container.Release(instance);
        public void BuildAndDispatch(Type typeToBuild, Action<object> action) =>
            (action ?? throw new ArgumentNullException(nameof(action)))(container.Build(typeToBuild));
        public void Dispose() { if (ownsContainer) container.Dispose(); }

        public void ConfigureComponent(Type concreteComponent, DependencyLifecycle dependencyLifecycle) =>
            container.Configure(concreteComponent, dependencyLifecycle);

        public void ConfigureComponent<T>(DependencyLifecycle dependencyLifecycle) =>
            container.Configure(typeof(T), dependencyLifecycle);

        public void ConfigureComponent<T>(Func<T> componentFactory, DependencyLifecycle dependencyLifecycle) =>
            container.Configure(componentFactory, dependencyLifecycle);

        public void ConfigureComponent<T>(Func<IBuilder, T> componentFactory, DependencyLifecycle dependencyLifecycle)
        {
            if (componentFactory == null) throw new ArgumentNullException(nameof(componentFactory));
            if (container is DefaultContainer defaultContainer)
                defaultContainer.Configure(scope => componentFactory(new ContainerBuilder(scope)), dependencyLifecycle);
            else container.Configure(() => componentFactory(this), dependencyLifecycle);
        }

        public bool HasComponent<T>() => container.HasComponent(typeof(T));
        public bool HasComponent(Type componentType) => container.HasComponent(componentType);
        public void RegisterSingleton(Type lookupType, object instance) => container.RegisterSingleton(lookupType, instance);
        public void RegisterSingleton<T>(T instance) => container.RegisterSingleton(typeof(T), instance);
    }
}

namespace Bq.Messaging
{
    public static class UseContainerExtensions
    {
        public static void UseContainer<T>(this EndpointConfiguration configuration, Action<ContainerCustomizations> customization = null)
            where T : ContainerDefinition, new()
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.UseContainer(typeof(T));
            customization?.Invoke(new ContainerCustomizations(configuration.GetSettings()));
        }
    }
}
