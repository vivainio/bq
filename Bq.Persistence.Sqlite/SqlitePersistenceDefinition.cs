using System;
using System.Data.Common;
using Bq.Messaging;
using Bq.Messaging.Configuration.AdvancedExtensibility;
using Bq.Messaging.Settings;

namespace Bq.Persistence.Sqlite
{
    /// <summary>Use with <c>UsePersistence&lt;SqlitePersistenceDefinition&gt;().ConnectionFactory(...)</c>.</summary>
    public sealed class SqlitePersistenceDefinition : PersistenceDefinition
    {
        internal const string FactoryKey = "Bq.Persistence.Sqlite.ConnectionFactory";

        public override IPersistenceProvider CreateProvider(ReadOnlySettings settings)
        {
            if (!settings.TryGet<Func<DbConnection>>(FactoryKey, out var factory))
                throw new InvalidOperationException("Call ConnectionFactory on UsePersistence<SqlitePersistenceDefinition>() first.");
            return new SqlitePersistence(factory, settings.Get<string>("Bq.EndpointName"));
        }
    }

    public static class SqlitePersistenceExtensions
    {
        public static PersistenceExtensions<SqlitePersistenceDefinition> ConnectionFactory(
            this PersistenceExtensions<SqlitePersistenceDefinition> extensions, Func<DbConnection> factory)
        {
            if (extensions == null) throw new ArgumentNullException(nameof(extensions));
            extensions.GetSettings().Set(SqlitePersistenceDefinition.FactoryKey, factory ?? throw new ArgumentNullException(nameof(factory)));
            return extensions;
        }
    }
}
