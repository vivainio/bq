using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Bq.Messaging.MultiTenant
{
    /// <summary>
    /// One persistence provider for many tenants, each with its own database. It opens a session on the database of the current
    /// <see cref="Tenant"/>, and it publishes and cleans up for all tenants in the background without querying every database each second.
    /// </summary>
    /// <remarks>
    /// A tenant's outbox is looked at straight after a session of that tenant commits rows, so the normal case costs no polling. Every other
    /// tenant is looked at less and less often while it stays idle, up to <see cref="TenantOptions.MaxIdleInterval"/>, and only a few tenants are
    /// looked at in each poll, so rows left by a crash are found without a query for every tenant every second. A tenant whose database fails is
    /// left alone for a while without holding up the others.
    /// </remarks>
    public sealed class TenantPersistence : IPersistenceProvider, IRecoveryPersistence, IOutgoingFailureTracking, IBatchOutgoingPersistence,
        ILockedDispatchPersistence, ILockedBatchDispatchPersistence, IRetentionPersistence, IInstallablePersistence
    {
        readonly Func<string, IPersistenceProvider> create;
        readonly TenantOptions options;
        readonly ConcurrentDictionary<string, IPersistenceProvider> providers = new ConcurrentDictionary<string, IPersistenceProvider>(StringComparer.Ordinal);
        readonly Schedule outbox = new Schedule();
        readonly Schedule recovery = new Schedule();
        // Which tenant a row or record returned to the dispatcher belongs to, so that marking it done goes to the same database.
        readonly ConcurrentDictionary<string, string> outgoingOwners = new ConcurrentDictionary<string, string>(StringComparer.Ordinal);
        readonly ConcurrentDictionary<string, string> recoveryOwners = new ConcurrentDictionary<string, string>(StringComparer.Ordinal);
        readonly SemaphoreSlim catalogGate = new SemaphoreSlim(1, 1);
        HashSet<string> catalog = new HashSet<string>(StringComparer.Ordinal);
        DateTime catalogAt = DateTime.MinValue;

        public TenantPersistence(Func<string, IPersistenceProvider> providerFor, TenantOptions options = null)
        {
            create = providerFor ?? throw new ArgumentNullException(nameof(providerFor));
            this.options = options ?? new TenantOptions();
            this.options.Validate();
        }

        internal string HeaderName => options.HeaderName;

        // ---- tenants -------------------------------------------------------------------------------------------------

        IPersistenceProvider Provider(string tenant) =>
            providers.GetOrAdd(tenant, key =>
            {
                var provider = create(key) ?? throw new InvalidOperationException("The provider factory returned null for tenant '" + key + "'.");
                if (!(provider is IRecoveryPersistence) || !(provider is IOutgoingFailureTracking) || !(provider is IBatchOutgoingPersistence))
                    throw new NotSupportedException("A tenant's provider must support recovery, failure tracking and batches, as the SQLite, Oracle and in-memory providers do.");
                return provider;
            });

        async Task<IReadOnlyCollection<string>> Catalog(bool force = false)
        {
            if (options.Tenants == null) return outbox.Tenants();
            await catalogGate.WaitAsync().ConfigureAwait(false);
            try
            {
                if (force || DateTime.UtcNow - catalogAt >= options.CatalogRefresh)
                {
                    var ids = await options.Tenants().ConfigureAwait(false) ?? new string[0];
                    var set = new HashSet<string>(ids.Where(id => !string.IsNullOrWhiteSpace(id)), StringComparer.Ordinal);
                    foreach (var id in set) { outbox.Add(id); recovery.Add(id); }
                    catalog = set;
                    catalogAt = DateTime.UtcNow;
                }
                return catalog;
            }
            finally { catalogGate.Release(); }
        }

        /// <summary>Throws <see cref="UnknownTenantException"/> when a catalogue is configured and does not list the tenant.</summary>
        internal async Task EnsureKnown(string tenant)
        {
            if (options.Tenants == null) return;
            if ((await Catalog().ConfigureAwait(false)).Contains(tenant)) return;
            // A tenant created a moment ago may not be in the cached catalogue yet.
            if ((await Catalog(force: true).ConfigureAwait(false)).Contains(tenant)) return;
            throw new UnknownTenantException(tenant);
        }

        void Register(string tenant) { outbox.Add(tenant); recovery.Add(tenant); }

        // ---- sessions ------------------------------------------------------------------------------------------------

        public async Task<IPersistenceSession> OpenSession()
        {
            var tenant = Tenant.Require();
            Register(tenant);
            var inner = await Provider(tenant).OpenSession().ConfigureAwait(false);
            return new Session(this, tenant, inner);
        }

        sealed class Session : IPersistenceSession, IRecoverySession
        {
            readonly TenantPersistence owner;
            readonly string tenant;
            readonly IPersistenceSession inner;
            bool wroteOutgoing, wroteRecovery;

            public Session(TenantPersistence owner, string tenant, IPersistenceSession inner)
            {
                this.owner = owner; this.tenant = tenant; this.inner = inner;
            }

            public DbConnection Connection => inner.Connection;
            public DbTransaction Transaction => inner.Transaction;
            public Task<bool> TryClaimMessage(string endpoint, string messageId) => inner.TryClaimMessage(endpoint, messageId);
            public Task StoreOutgoing(OutgoingMessage message) { wroteOutgoing = true; return inner.StoreOutgoing(message); }
            public Task<SagaRecord> LoadSaga(string sagaType, string sagaId) => inner.LoadSaga(sagaType, sagaId);
            public Task SaveSaga(string sagaType, string sagaId, byte[] data, long? expectedVersion) => inner.SaveSaga(sagaType, sagaId, data, expectedVersion);
            public Task DeleteSaga(string sagaType, string sagaId, long expectedVersion) => inner.DeleteSaga(sagaType, sagaId, expectedVersion);
            public Task<bool> HasRecovery(string endpoint, string recoveryId) => Recovery.HasRecovery(endpoint, recoveryId);
            public Task StoreRecovery(string endpoint, RecoveryRecord record) { wroteRecovery = true; return Recovery.StoreRecovery(endpoint, record); }
            IRecoverySession Recovery => inner as IRecoverySession ?? throw new NotSupportedException("The tenant's session does not support recovery.");

            public void Commit()
            {
                inner.Commit();
                // New rows: look at this tenant's outbox on the next poll, ahead of idle tenants.
                if (wroteOutgoing) owner.outbox.Hot(tenant);
                if (wroteRecovery) owner.recovery.Hot(tenant);
            }

            public void Dispose() => inner.Dispose();
        }

        // ---- dispatching ---------------------------------------------------------------------------------------------

        // Looks at the tenants that are due, hot ones first and at most ProbesPerPoll of them, and returns the first one that has work. A tenant
        // whose database fails is set aside, and the failure surfaces only when every tenant that was tried failed.
        async Task<(bool Hit, T Result)> FirstHit<T>(Schedule schedule, Func<string, Task<(bool Hit, T Result)>> attempt)
        {
            await Catalog().ConfigureAwait(false);
            var due = schedule.Due(options.ProbesPerPoll);
            ExceptionDispatchInfo firstFailure = null;
            var failed = 0;
            foreach (var tenant in due)
            {
                try
                {
                    (bool Hit, T Result) found;
                    using (Tenant.Scope(tenant)) found = await attempt(tenant).ConfigureAwait(false);
                    if (found.Hit) { schedule.Hot(tenant); return found; }
                    schedule.Miss(tenant, options.MaxIdleInterval);
                }
                catch (Exception ex)
                {
                    failed++;
                    if (firstFailure == null) firstFailure = ExceptionDispatchInfo.Capture(ex);
                    schedule.Fail(tenant, options.FailureBackoff);
                    System.Diagnostics.Trace.TraceError("Tenant '{0}' could not be dispatched and is left alone for {1}: {2}", tenant, options.FailureBackoff, ex.Message);
                }
            }
            if (due.Count > 0 && failed == due.Count) firstFailure.Throw();
            return (false, default(T));
        }

        string Owner(ConcurrentDictionary<string, string> owners, string id) =>
            owners.TryGetValue(id, out var tenant) ? tenant
                : throw new InvalidOperationException("'" + id + "' was not handed out by this provider, so its tenant is not known.");

        public async Task<OutgoingMessage> NextOutgoing(string endpoint) =>
            (await FirstHit<OutgoingMessage>(outbox, async tenant =>
            {
                var row = await Provider(tenant).NextOutgoing(endpoint).ConfigureAwait(false);
                if (row != null) outgoingOwners[row.Id] = tenant;
                return (row != null, row);
            }).ConfigureAwait(false)).Result;

        public async Task MarkDispatched(string endpoint, string messageId)
        {
            var tenant = Owner(outgoingOwners, messageId);
            using (Tenant.Scope(tenant)) await Provider(tenant).MarkDispatched(endpoint, messageId).ConfigureAwait(false);
            outgoingOwners.TryRemove(messageId, out _);
        }

        public async Task RecordOutgoingFailure(string endpoint, string messageId, Exception failure)
        {
            var tenant = Owner(outgoingOwners, messageId);
            using (Tenant.Scope(tenant)) await ((IOutgoingFailureTracking)Provider(tenant)).RecordOutgoingFailure(endpoint, messageId, failure).ConfigureAwait(false);
        }

        public async Task<IReadOnlyList<OutgoingMessage>> NextOutgoingBatch(string endpoint, int maxCount) =>
            (await FirstHit<IReadOnlyList<OutgoingMessage>>(outbox, async tenant =>
            {
                var rows = await ((IBatchOutgoingPersistence)Provider(tenant)).NextOutgoingBatch(endpoint, maxCount).ConfigureAwait(false);
                foreach (var row in rows) outgoingOwners[row.Id] = tenant;
                return (rows.Count > 0, rows);
            }).ConfigureAwait(false)).Result ?? new OutgoingMessage[0];

        public async Task MarkDispatched(string endpoint, IReadOnlyCollection<string> messageIds)
        {
            foreach (var group in messageIds.GroupBy(id => Owner(outgoingOwners, id)))
            {
                using (Tenant.Scope(group.Key))
                    await ((IBatchOutgoingPersistence)Provider(group.Key)).MarkDispatched(endpoint, group.ToList()).ConfigureAwait(false);
                foreach (var id in group) outgoingOwners.TryRemove(id, out _);
            }
        }

        public async Task<bool> DispatchOutgoing(string endpoint, Action<OutgoingMessage> publish) =>
            (await FirstHit<bool>(outbox, async tenant =>
            {
                var inner = Provider(tenant);
                if (inner is ILockedDispatchPersistence locked)
                {
                    var done = await locked.DispatchOutgoing(endpoint, publish).ConfigureAwait(false);
                    return (done, done);
                }
                var row = await inner.NextOutgoing(endpoint).ConfigureAwait(false);
                if (row == null) return (false, false);
                try { publish(row); }
                catch (OutgoingMessageRejectedException rejected)
                {
                    await ((IOutgoingFailureTracking)inner).RecordOutgoingFailure(endpoint, row.Id, rejected).ConfigureAwait(false);
                    return (true, true);
                }
                await inner.MarkDispatched(endpoint, row.Id).ConfigureAwait(false);
                return (true, true);
            }).ConfigureAwait(false)).Hit;

        public async Task<int> DispatchOutgoingBatch(string endpoint, int maxCount, Func<IReadOnlyList<OutgoingMessage>, IReadOnlyCollection<string>> publish) =>
            (await FirstHit<int>(outbox, async tenant =>
            {
                var inner = Provider(tenant);
                if (inner is ILockedBatchDispatchPersistence locked)
                {
                    var count = await locked.DispatchOutgoingBatch(endpoint, maxCount, publish).ConfigureAwait(false);
                    return (count > 0, count);
                }
                var rows = await ((IBatchOutgoingPersistence)inner).NextOutgoingBatch(endpoint, maxCount).ConfigureAwait(false);
                if (rows.Count == 0) return (false, 0);
                var rejected = new HashSet<string>(publish(rows) ?? new string[0], StringComparer.Ordinal);
                foreach (var id in rejected)
                    await ((IOutgoingFailureTracking)inner).RecordOutgoingFailure(endpoint, id, new OutgoingMessageRejectedException(id, "Message was unroutable.")).ConfigureAwait(false);
                var sent = rows.Select(row => row.Id).Where(id => !rejected.Contains(id)).ToList();
                if (sent.Count > 0) await ((IBatchOutgoingPersistence)inner).MarkDispatched(endpoint, sent).ConfigureAwait(false);
                return (true, rows.Count);
            }).ConfigureAwait(false)).Result;

        // ---- recovery ------------------------------------------------------------------------------------------------

        public async Task<RecoveryRecord> NextRecovery(string endpoint) =>
            (await FirstHit<RecoveryRecord>(recovery, async tenant =>
            {
                var record = await ((IRecoveryPersistence)Provider(tenant)).NextRecovery(endpoint).ConfigureAwait(false);
                if (record != null) recoveryOwners[record.RecoveryId] = tenant;
                return (record != null, record);
            }).ConfigureAwait(false)).Result;

        public async Task MarkRecoveryDispatched(string endpoint, string recoveryId)
        {
            var tenant = Owner(recoveryOwners, recoveryId);
            using (Tenant.Scope(tenant)) await ((IRecoveryPersistence)Provider(tenant)).MarkRecoveryDispatched(endpoint, recoveryId).ConfigureAwait(false);
            recoveryOwners.TryRemove(recoveryId, out _);
        }

        public async Task<bool> DispatchRecovery(string endpoint, Action<RecoveryRecord> publish) =>
            (await FirstHit<bool>(recovery, async tenant =>
            {
                var inner = Provider(tenant);
                if (inner is ILockedDispatchPersistence locked)
                {
                    var done = await locked.DispatchRecovery(endpoint, publish).ConfigureAwait(false);
                    return (done, done);
                }
                var store = (IRecoveryPersistence)inner;
                var record = await store.NextRecovery(endpoint).ConfigureAwait(false);
                if (record == null) return (false, false);
                publish(record);
                await store.MarkRecoveryDispatched(endpoint, record.RecoveryId).ConfigureAwait(false);
                return (true, true);
            }).ConfigureAwait(false)).Hit;

        // Inspection and replay are for one tenant, the current one.
        public Task<RecoveryRecord> GetError(string endpoint, string recoveryId) =>
            ((IRecoveryPersistence)Provider(Tenant.Require())).GetError(endpoint, recoveryId);

        public Task<IReadOnlyList<RecoveryRecord>> ListErrors(string endpoint, int limit) =>
            ((IRecoveryPersistence)Provider(Tenant.Require())).ListErrors(endpoint, limit);

        public async Task<bool> ReplayError(string endpoint, string recoveryId, RecoveryRecord replay)
        {
            var tenant = Tenant.Require();
            var replayed = await ((IRecoveryPersistence)Provider(tenant)).ReplayError(endpoint, recoveryId, replay).ConfigureAwait(false);
            if (replayed) recovery.Hot(tenant);
            return replayed;
        }

        // ---- retention and set-up ------------------------------------------------------------------------------------

        public Task<int> PurgeInbox(string endpoint, DateTime claimedBeforeUtc, int maxRows) =>
            Maintain(maxRows, (inner, remaining) => ((IRetentionPersistence)inner).PurgeInbox(endpoint, claimedBeforeUtc, remaining));

        public Task<int> PurgeRecovery(string endpoint, DateTime failedBeforeUtc, int maxRows) =>
            Maintain(maxRows, (inner, remaining) => ((IRetentionPersistence)inner).PurgeRecovery(endpoint, failedBeforeUtc, remaining));

        // Runs a purge on each tenant in turn until the row budget is spent. A tenant that fails is skipped.
        async Task<int> Maintain(int maxRows, Func<IPersistenceProvider, int, Task<int>> purge)
        {
            var removed = 0;
            foreach (var tenant in (await Catalog().ConfigureAwait(false)).ToList())
            {
                if (removed >= maxRows) break;
                try
                {
                    if (!(Provider(tenant) is IRetentionPersistence)) continue;
                    using (Tenant.Scope(tenant)) removed += await purge(Provider(tenant), maxRows - removed).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Trace.TraceError("Retention for tenant '{0}' failed and is tried again on the next round: {1}", tenant, ex.Message);
                }
            }
            return removed;
        }

        /// <summary>Creates or upgrades the schema of every tenant in the catalogue, a few at a time. Without a catalogue it does nothing.</summary>
        public async Task Initialize()
        {
            if (options.Tenants == null) return;
            var tenants = (await Catalog(force: true).ConfigureAwait(false)).ToList();
            using (var limit = new SemaphoreSlim(options.MaintenanceConcurrency))
                await Task.WhenAll(tenants.Select(async tenant =>
                {
                    await limit.WaitAsync().ConfigureAwait(false);
                    try { await InitializeTenant(tenant).ConfigureAwait(false); }
                    finally { limit.Release(); }
                })).ConfigureAwait(false);
        }

        /// <summary>Creates or upgrades the schema of one tenant, for example when it is onboarded.</summary>
        public async Task InitializeTenant(string tenant)
        {
            if (string.IsNullOrWhiteSpace(tenant)) throw new ArgumentException("A tenant is required.", nameof(tenant));
            if (Provider(tenant) is IInstallablePersistence installable)
                using (Tenant.Scope(tenant)) await installable.Initialize().ConfigureAwait(false);
            Register(tenant);
        }

        // ---- the schedule of which tenant to look at next ------------------------------------------------------------

        sealed class Schedule
        {
            sealed class Entry { public DateTime Next; public TimeSpan Idle; }
            readonly object gate = new object();
            readonly Dictionary<string, Entry> entries = new Dictionary<string, Entry>(StringComparer.Ordinal);

            public void Add(string tenant) { lock (gate) if (!entries.ContainsKey(tenant)) entries[tenant] = new Entry { Next = DateTime.UtcNow }; }

            public IReadOnlyCollection<string> Tenants() { lock (gate) return entries.Keys.ToList(); }

            // Work was just found or written: look here again at once, ahead of tenants that are only being swept.
            public void Hot(string tenant) { lock (gate) entries[tenant] = new Entry { Next = DateTime.UtcNow }; }

            // Nothing was there: wait a second, then twice as long each time, up to the limit.
            public void Miss(string tenant, TimeSpan max)
            {
                lock (gate)
                {
                    if (!entries.TryGetValue(tenant, out var entry)) entries[tenant] = entry = new Entry();
                    var idle = entry.Idle == TimeSpan.Zero ? TimeSpan.FromSeconds(1) : TimeSpan.FromTicks(entry.Idle.Ticks * 2);
                    entry.Idle = idle > max ? max : idle;
                    entry.Next = DateTime.UtcNow + entry.Idle;
                }
            }

            public void Fail(string tenant, TimeSpan backoff)
            {
                lock (gate)
                {
                    if (!entries.TryGetValue(tenant, out var entry)) entries[tenant] = entry = new Entry();
                    entry.Idle = backoff;
                    entry.Next = DateTime.UtcNow + backoff;
                }
            }

            // Hot tenants first, oldest first, then the tenants whose wait is over, longest overdue first.
            public List<string> Due(int max)
            {
                var now = DateTime.UtcNow;
                lock (gate)
                    return entries.Where(pair => pair.Value.Next <= now)
                        .OrderBy(pair => pair.Value.Idle == TimeSpan.Zero ? 0 : 1).ThenBy(pair => pair.Value.Next)
                        .Take(max).Select(pair => pair.Key).ToList();
            }
        }
    }
}
