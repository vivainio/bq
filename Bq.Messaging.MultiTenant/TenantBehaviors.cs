using System;
using System.Threading.Tasks;

namespace Bq.Messaging.MultiTenant
{
    /// <summary>
    /// Reads the tenant from the delivery's header and makes it the current <see cref="Tenant"/> for everything Bq does with the delivery. A
    /// delivery with no tenant, or with one the catalogue does not list, is rejected: it goes to the error queue without a database.
    /// </summary>
    public sealed class TenantDeliveryBehavior : Behavior<IDeliveryContext>
    {
        readonly string header;
        readonly TenantPersistence persistence;

        public TenantDeliveryBehavior(TenantPersistence persistence)
        {
            this.persistence = persistence ?? throw new ArgumentNullException(nameof(persistence));
            header = persistence.HeaderName;
        }

        public override async Task Invoke(IDeliveryContext context, Func<Task> next)
        {
            if (!context.Headers.TryGetValue(header, out var tenant) || string.IsNullOrWhiteSpace(tenant))
                throw new MissingTenantException(header, context.MessageId);
            await persistence.EnsureKnown(tenant).ConfigureAwait(false);
            using (Tenant.Scope(tenant)) await next().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Puts the current tenant on every message Bq sends, replies to or publishes, including delayed ones, so it comes back with them. A message
    /// with no tenant, and no tenant set, is refused here and not left for the receiver to reject, because a send outside a handler does not open a
    /// database session that would otherwise notice.
    /// </summary>
    public sealed class TenantPropagationBehavior : Behavior<IOutgoingLogicalMessageContext>
    {
        readonly string header;

        public TenantPropagationBehavior(string header) { this.header = header ?? throw new ArgumentNullException(nameof(header)); }

        public override Task Invoke(IOutgoingLogicalMessageContext context, Func<Task> next)
        {
            if (!context.Headers.ContainsKey(header)) context.Headers[header] = Tenant.Require();
            return next();
        }
    }

    public static class TenantConfiguration
    {
        /// <summary>
        /// Makes the endpoint multi-tenant. <paramref name="providerFor"/> returns the persistence provider of one tenant, for example a
        /// <c>SqlitePersistence</c> or <c>OraclePersistence</c> over that tenant's connection string. It is called once for each tenant.
        /// </summary>
        public static TenantPersistence UseTenants(this EndpointConfiguration configuration, Func<string, IPersistenceProvider> providerFor,
            Action<TenantOptions> configure = null)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            var options = new TenantOptions();
            configure?.Invoke(options);
            var persistence = new TenantPersistence(providerFor, options);
            configuration.Persistence = persistence;
            configuration.Pipeline.Register("bq-tenant-delivery", new TenantDeliveryBehavior(persistence), "Make the tenant of the delivery current");
            configuration.Pipeline.Register("bq-tenant-propagation", new TenantPropagationBehavior(options.HeaderName), "Put the current tenant on outgoing messages");
            return persistence;
        }
    }
}
