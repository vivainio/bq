using System;
using System.Threading.Tasks;
using Bq.Messaging.ObjectBuilder;
using Bq.Messaging.Settings;

namespace Bq.Messaging
{
    /// <summary>A scanned type that customizes the endpoint configuration before the endpoint is created.</summary>
    public interface INeedInitialization
    {
        void Customize(EndpointConfiguration configuration);
    }

    public interface IWantToRunBeforeConfigurationIsFinalized
    {
        void Run(SettingsHolder config);
    }

    public interface IStartableEndpointWithExternallyManagedContainer
    {
        Lazy<IMessageSession> MessageSession { get; }
        Task<IEndpointInstance> Start(IBuilder externalBuilder);
    }

    /// <summary>
    /// For hosts that own the container: <see cref="Create"/> registers the endpoint's components with it, and
    /// <c>Start</c> receives the builder the host created from it.
    /// </summary>
    public static class EndpointWithExternallyManagedContainer
    {
        public static IStartableEndpointWithExternallyManagedContainer Create(EndpointConfiguration configuration, IConfigureComponents externalConfiguration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (externalConfiguration == null) throw new ArgumentNullException(nameof(externalConfiguration));
            if (!configuration.IsSendOnly) configuration.ApplyAssemblyScanning();
            configuration.ApplyTransportSettings();
            configuration.ApplyPersistenceSettings();
            configuration.ActivateFeatures();
            configuration.RegisterComponentsWith(externalConfiguration);
            return new Startable(configuration);
        }

        sealed class Startable : IStartableEndpointWithExternallyManagedContainer
        {
            readonly EndpointConfiguration configuration;
            RabbitEndpoint endpoint;

            public Startable(EndpointConfiguration configuration)
            {
                this.configuration = configuration;
                MessageSession = new Lazy<IMessageSession>(() => endpoint ?? throw new InvalidOperationException("The endpoint has not started."));
            }

            public Lazy<IMessageSession> MessageSession { get; }

            public Task<IEndpointInstance> Start(IBuilder externalBuilder)
            {
                configuration.UseExternalBuilder(externalBuilder ?? throw new ArgumentNullException(nameof(externalBuilder)));
                endpoint = RabbitEndpoint.Start(configuration);
                return Task.FromResult<IEndpointInstance>(endpoint);
            }
        }
    }

    public sealed partial class EndpointConfiguration
    {
        internal IBuilder ExternalBuilder { get; private set; }

        internal void UseExternalBuilder(IBuilder builder) => ExternalBuilder = builder;

        /// <summary>Hands the registrations collected so far to a container the host owns, and forgets them.</summary>
        internal void RegisterComponentsWith(IConfigureComponents components)
        {
            foreach (var registration in componentRegistrations) registration(components);
            componentRegistrations.Clear();
        }
    }
}
