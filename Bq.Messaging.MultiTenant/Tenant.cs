using System;
using System.Threading;

namespace Bq.Messaging.MultiTenant
{
    /// <summary>
    /// The tenant the current work is for. Bq sets it around each delivery, so handlers, persistence sessions and failure handling see it. Code that
    /// runs outside a delivery, such as a web request that sends a message, sets it with <see cref="Scope"/>. It is an <see cref="AsyncLocal{T}"/>,
    /// so it flows through <c>await</c> and into tasks started inside the scope.
    /// </summary>
    public static class Tenant
    {
        static readonly AsyncLocal<string> current = new AsyncLocal<string>();

        /// <summary>The current tenant, or null when none is set.</summary>
        public static string Current => current.Value;

        /// <summary>The current tenant. It throws when none is set, so work with no tenant can never fall back to another tenant's data.</summary>
        public static string Require() =>
            current.Value ?? throw new TenantNotSetException();

        /// <summary>Makes <paramref name="tenant"/> the current tenant until the returned scope is disposed.</summary>
        public static IDisposable Scope(string tenant)
        {
            if (string.IsNullOrWhiteSpace(tenant)) throw new ArgumentException("A tenant is required.", nameof(tenant));
            var previous = current.Value;
            current.Value = tenant;
            return new Restore(previous);
        }

        sealed class Restore : IDisposable
        {
            readonly string previous;
            int disposed;
            public Restore(string previous) { this.previous = previous; }
            public void Dispose()
            {
                if (Interlocked.Exchange(ref disposed, 1) == 0) current.Value = previous;
            }
        }
    }

    /// <summary>Work needed a tenant and none was set.</summary>
    public sealed class TenantNotSetException : InvalidOperationException
    {
        public TenantNotSetException() : base("No tenant is set. Wrap the work in Tenant.Scope(tenant), or run it inside a delivery.") { }
    }

    /// <summary>A delivery carried no tenant header.</summary>
    public sealed class MissingTenantException : InvalidOperationException
    {
        public MissingTenantException(string header, string messageId) : base("Message " + messageId + " has no '" + header + "' header, so it has no tenant.") { }
    }

    /// <summary>A delivery named a tenant that is not in the tenant catalogue.</summary>
    public sealed class UnknownTenantException : InvalidOperationException
    {
        public UnknownTenantException(string tenant) : base("Tenant '" + tenant + "' is not known.") { Tenant = tenant; }
        public string Tenant { get; }
    }
}
