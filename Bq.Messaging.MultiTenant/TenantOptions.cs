using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Bq.Messaging.MultiTenant
{
    public sealed class TenantOptions
    {
        /// <summary>The header that carries the tenant. The default is <c>tenant-id</c>.</summary>
        public string HeaderName { get; set; } = "tenant-id";

        /// <summary>
        /// Returns every tenant ID. It is used to refuse unknown tenants, to publish rows left behind by a crash, and for retention and schema
        /// set-up. Without it, only tenants that have been active since the endpoint started are looked after in the background.
        /// </summary>
        public Func<Task<IReadOnlyList<string>>> Tenants { get; set; }

        /// <summary>How long the catalogue is kept before it is read again. The default is one minute.</summary>
        public TimeSpan CatalogRefresh { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>The longest an idle tenant waits between looks at its database. The wait grows from a second up to this. The default is 30 seconds.</summary>
        public TimeSpan MaxIdleInterval { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>How many tenant databases one dispatch poll may look at. The default is 8, so a poll never queries every tenant.</summary>
        public int ProbesPerPoll { get; set; } = 8;

        /// <summary>How long a tenant whose database failed is left alone before it is tried again. The default is 30 seconds.</summary>
        public TimeSpan FailureBackoff { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>How many tenants retention and set-up work on at once. The default is 4.</summary>
        public int MaintenanceConcurrency { get; set; } = 4;

        internal void Validate()
        {
            if (string.IsNullOrWhiteSpace(HeaderName)) throw new ArgumentException("A tenant header name is required.", nameof(HeaderName));
            if (CatalogRefresh <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(CatalogRefresh));
            if (MaxIdleInterval < TimeSpan.FromSeconds(1)) throw new ArgumentOutOfRangeException(nameof(MaxIdleInterval), "The idle interval must be at least a second.");
            if (ProbesPerPoll < 1) throw new ArgumentOutOfRangeException(nameof(ProbesPerPoll));
            if (FailureBackoff <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(FailureBackoff));
            if (MaintenanceConcurrency < 1) throw new ArgumentOutOfRangeException(nameof(MaintenanceConcurrency));
        }
    }
}
