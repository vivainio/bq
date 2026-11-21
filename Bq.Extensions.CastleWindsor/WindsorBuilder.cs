using System;
using System.Collections.Generic;
using System.Linq;
using Bq.Messaging;
using Bq.Messaging.Container;
using Bq.Messaging.ObjectBuilder;
using Bq.Messaging.ObjectBuilder.Common;
using Bq.Messaging.Settings;
using Castle.MicroKernel.Lifestyle;
using Castle.MicroKernel.Registration;
using Castle.Windsor;

namespace Bq.Extensions.CastleWindsor
{
    /// <summary>
    /// Use with <c>UseContainer&lt;WindsorBuilder&gt;(c =&gt; c.ExistingContainer(container))</c>. Without an existing
    /// container Bq creates a new <see cref="WindsorContainer"/> and disposes it with the endpoint.
    /// </summary>
    public sealed class WindsorBuilder : ContainerDefinition
    {
        internal const string ExistingContainerKey = "Bq.Extensions.CastleWindsor.ExistingContainer";

        public override IContainer CreateContainer(ReadOnlySettings settings)
        {
            if (settings != null && settings.TryGet<IWindsorContainer>(ExistingContainerKey, out var existing))
                return new WindsorContainerAdapter(existing, ownsContainer: false, scope: null);
            return new WindsorContainerAdapter(new WindsorContainer(), ownsContainer: true, scope: null);
        }
    }

    public static class WindsorBuilderExtensions
    {
        public static void ExistingContainer(this ContainerCustomizations customizations, IWindsorContainer existingContainer)
        {
            if (customizations == null) throw new ArgumentNullException(nameof(customizations));
            customizations.Settings.Set(WindsorBuilder.ExistingContainerKey, existingContainer ?? throw new ArgumentNullException(nameof(existingContainer)));
        }
    }

    sealed class WindsorContainerAdapter : IContainer
    {
        readonly IWindsorContainer container;
        readonly bool ownsContainer;
        readonly IDisposable scope;

        public WindsorContainerAdapter(IWindsorContainer container, bool ownsContainer, IDisposable scope)
        {
            this.container = container;
            this.ownsContainer = ownsContainer;
            this.scope = scope;
            if (scope == null && !container.Kernel.HasComponent(typeof(IBuilder)))
                container.Register(Component.For<IBuilder>().UsingFactoryMethod(() => new ContainerBuilder(this)).LifestyleTransient());
        }

        static Type[] Services(Type implementation) =>
            new[] { implementation }.Concat(implementation.GetInterfaces()).Distinct().ToArray();

        static ComponentRegistration<object> Lifestyle(ComponentRegistration<object> registration, DependencyLifecycle lifecycle)
        {
            switch (lifecycle)
            {
                case DependencyLifecycle.SingleInstance: return registration.LifestyleSingleton();
                case DependencyLifecycle.InstancePerUnitOfWork: return registration.LifestyleScoped();
                default: return registration.LifestyleTransient();
            }
        }

        public object Build(Type typeToBuild) => container.Resolve(typeToBuild);

        public IEnumerable<object> BuildAll(Type typeToBuild) => container.ResolveAll(typeToBuild).Cast<object>().ToList();

        public IContainer BuildChildContainer() => new WindsorContainerAdapter(container, ownsContainer: false, scope: container.BeginScope());

        public void Configure(Type component, DependencyLifecycle dependencyLifecycle) =>
            container.Register(Lifestyle(Component.For(Services(component)).ImplementedBy(component), dependencyLifecycle));

        public void Configure<T>(Func<T> component, DependencyLifecycle dependencyLifecycle) =>
            container.Register(Lifestyle(Component.For(Services(typeof(T))).UsingFactoryMethod(() => (object)component()), dependencyLifecycle));

        public bool HasComponent(Type componentType) => container.Kernel.HasComponent(componentType);

        public void RegisterSingleton(Type lookupType, object instance) =>
            container.Register(Component.For(new[] { lookupType }.Concat(instance.GetType().GetInterfaces()).Distinct().ToArray()).Instance(instance));

        public void Release(object instance) => container.Release(instance);

        public void Dispose()
        {
            scope?.Dispose();
            if (ownsContainer) container.Dispose();
        }
    }
}
