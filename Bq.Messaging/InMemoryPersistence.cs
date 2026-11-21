using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging.Settings;

namespace Bq.Messaging
{
    /// <summary>
    /// Keeps the inbox, outbox, timeouts, sagas and recovery records in memory. Everything is lost when the process ends, so use it
    /// for tests and local runs. Sessions have no database connection, so handlers that need <c>Connection</c> or <c>Transaction</c> cannot use it.
    /// </summary>
    public sealed class InMemoryPersistence : PersistenceDefinition
    {
        public override IPersistenceProvider CreateProvider(ReadOnlySettings settings) =>
            new InMemoryPersistenceProvider(settings.Get<string>(EndpointConfiguration.EndpointNameKey));
    }

    public sealed class InMemoryPersistenceProvider : IPersistenceProvider, IRecoveryPersistence, IInstallablePersistence
    {
        readonly object gate = new object();
        readonly string endpoint;
        readonly HashSet<string> inbox = new HashSet<string>(StringComparer.Ordinal);
        readonly HashSet<string> reserved = new HashSet<string>(StringComparer.Ordinal);
        readonly List<OutgoingMessage> outbox = new List<OutgoingMessage>();
        readonly List<(OutgoingMessage Message, DateTime Due)> timeouts = new List<(OutgoingMessage, DateTime)>();
        readonly Dictionary<string, RecoveryRecord> recovery = new Dictionary<string, RecoveryRecord>(StringComparer.Ordinal);
        readonly HashSet<string> replayed = new HashSet<string>(StringComparer.Ordinal);
        readonly Dictionary<(string Type, string Id), SagaRecord> sagas = new Dictionary<(string, string), SagaRecord>();
        int outgoingPoll;

        public InMemoryPersistenceProvider(string endpoint)
        {
            this.endpoint = !string.IsNullOrWhiteSpace(endpoint) ? endpoint : throw new ArgumentException("Endpoint is required.", nameof(endpoint));
        }

        public Task Initialize() => Task.CompletedTask;

        void Check(string name)
        {
            if (name != endpoint) throw new InvalidOperationException("Persistence endpoint mismatch.");
        }

        public Task<IPersistenceSession> OpenSession() => Task.FromResult<IPersistenceSession>(new Session(this));

        public Task<OutgoingMessage> NextOutgoing(string name)
        {
            Check(name);
            lock (gate)
            {
                // Alternate between categories so a stuck head in one cannot starve the other.
                var preferTimeout = (Interlocked.Increment(ref outgoingPoll) & 1) == 1;
                var due = timeouts.Where(entry => entry.Due <= DateTime.UtcNow).OrderBy(entry => entry.Due).ThenBy(entry => entry.Message.Id, StringComparer.Ordinal)
                    .Select(entry => entry.Message).FirstOrDefault();
                var queued = outbox.FirstOrDefault();
                return Task.FromResult(preferTimeout ? due ?? queued : queued ?? due);
            }
        }

        public Task MarkDispatched(string name, string id)
        {
            Check(name);
            lock (gate)
            {
                outbox.RemoveAll(message => message.Id == id);
                timeouts.RemoveAll(entry => entry.Message.Id == id);
            }
            return Task.CompletedTask;
        }

        public Task<RecoveryRecord> NextRecovery(string name)
        {
            Check(name);
            lock (gate)
                return Task.FromResult(recovery.Values.Where(record => !record.Dispatched && record.DueAtUtc <= DateTime.UtcNow)
                    .OrderBy(record => record.DueAtUtc).ThenBy(record => record.RecoveryId, StringComparer.Ordinal).FirstOrDefault());
        }

        public Task MarkRecoveryDispatched(string name, string recoveryId)
        {
            Check(name);
            lock (gate)
                if (recovery.TryGetValue(recoveryId, out var record)) record.Dispatched = true;
            return Task.CompletedTask;
        }

        public Task<RecoveryRecord> GetError(string name, string recoveryId)
        {
            Check(name);
            lock (gate)
                return Task.FromResult(recovery.TryGetValue(recoveryId, out var record) && record.IsError && !replayed.Contains(recoveryId) ? record : null);
        }

        public Task<IReadOnlyList<RecoveryRecord>> ListErrors(string name, int limit)
        {
            Check(name);
            if (limit <= 0) throw new ArgumentOutOfRangeException(nameof(limit));
            lock (gate)
                return Task.FromResult<IReadOnlyList<RecoveryRecord>>(recovery.Values.Where(record => record.IsError && !replayed.Contains(record.RecoveryId))
                    .OrderByDescending(record => record.FailedAtUtc).ThenByDescending(record => record.RecoveryId, StringComparer.Ordinal).Take(limit).ToList());
        }

        public Task<bool> ReplayError(string name, string recoveryId, RecoveryRecord replay)
        {
            Check(name);
            lock (gate)
            {
                if (!recovery.TryGetValue(recoveryId, out var record) || !record.IsError || replayed.Contains(recoveryId)) return Task.FromResult(false);
                replayed.Add(recoveryId);
                if (!recovery.ContainsKey(replay.RecoveryId)) recovery.Add(replay.RecoveryId, replay);
                return Task.FromResult(true);
            }
        }

        sealed class SagaChange
        {
            public byte[] Data;
            public long Version;
            public bool Deleted;
            public long? CommittedVersion;
        }

        sealed class Session : IPersistenceSession, IRecoverySession
        {
            readonly InMemoryPersistenceProvider owner;
            readonly HashSet<string> claims = new HashSet<string>(StringComparer.Ordinal);
            readonly List<OutgoingMessage> outgoing = new List<OutgoingMessage>();
            readonly List<(OutgoingMessage, DateTime)> scheduled = new List<(OutgoingMessage, DateTime)>();
            readonly List<RecoveryRecord> recoveries = new List<RecoveryRecord>();
            readonly Dictionary<(string, string), SagaChange> changes = new Dictionary<(string, string), SagaChange>();
            bool finished;

            public Session(InMemoryPersistenceProvider owner) { this.owner = owner; }

            public DbConnection Connection => null;
            public DbTransaction Transaction => null;

            public Task<bool> TryClaimMessage(string name, string messageId)
            {
                owner.Check(name);
                if (string.IsNullOrEmpty(messageId)) throw new InvalidOperationException("A message ID is required for deduplication.");
                lock (owner.gate)
                {
                    if (owner.inbox.Contains(messageId) || owner.reserved.Contains(messageId)) return Task.FromResult(false);
                    owner.reserved.Add(messageId);
                    claims.Add(messageId);
                    return Task.FromResult(true);
                }
            }

            public Task StoreOutgoing(OutgoingMessage message) { outgoing.Add(message); return Task.CompletedTask; }

            public Task StoreTimeout(OutgoingMessage message, DateTime dueUtc)
            {
                if (dueUtc.Kind != DateTimeKind.Utc) throw new ArgumentException("Timeout must use UTC.", nameof(dueUtc));
                scheduled.Add((message, dueUtc));
                return Task.CompletedTask;
            }

            public Task<bool> HasRecovery(string name, string recoveryId)
            {
                owner.Check(name);
                lock (owner.gate) return Task.FromResult(owner.recovery.ContainsKey(recoveryId) || recoveries.Any(record => record.RecoveryId == recoveryId));
            }

            public Task StoreRecovery(string name, RecoveryRecord record)
            {
                owner.Check(name);
                if (!recoveries.Any(existing => existing.RecoveryId == record.RecoveryId)) recoveries.Add(record);
                return Task.CompletedTask;
            }

            SagaRecord Current(string type, string id)
            {
                if (changes.TryGetValue((type, id), out var change))
                    return change.Deleted ? null : new SagaRecord { Data = change.Data, Version = change.Version };
                lock (owner.gate) return owner.sagas.TryGetValue((type, id), out var stored) ? new SagaRecord { Data = stored.Data, Version = stored.Version } : null;
            }

            SagaChange Change(string type, string id)
            {
                if (changes.TryGetValue((type, id), out var change)) return change;
                lock (owner.gate)
                    changes.Add((type, id), change = new SagaChange { CommittedVersion = owner.sagas.TryGetValue((type, id), out var stored) ? stored.Version : (long?)null });
                return change;
            }

            public Task<SagaRecord> LoadSaga(string sagaType, string sagaId) => Task.FromResult(Current(sagaType, sagaId));

            public Task SaveSaga(string sagaType, string sagaId, byte[] data, long? expectedVersion)
            {
                var current = Current(sagaType, sagaId);
                if (expectedVersion.HasValue ? current == null || current.Version != expectedVersion.Value : current != null)
                    throw new SagaConcurrencyException("Saga was changed concurrently.");
                var change = Change(sagaType, sagaId);
                change.Data = (byte[])data.Clone();
                change.Version = (expectedVersion ?? 0) + 1;
                change.Deleted = false;
                return Task.CompletedTask;
            }

            public Task DeleteSaga(string sagaType, string sagaId, long expectedVersion)
            {
                var current = Current(sagaType, sagaId);
                if (current == null || current.Version != expectedVersion) throw new SagaConcurrencyException("Saga was changed concurrently.");
                Change(sagaType, sagaId).Deleted = true;
                return Task.CompletedTask;
            }

            public void Commit()
            {
                lock (owner.gate)
                {
                    foreach (var pair in changes)
                    {
                        var committed = owner.sagas.TryGetValue(pair.Key, out var stored) ? stored.Version : (long?)null;
                        if (committed != pair.Value.CommittedVersion) throw new SagaConcurrencyException("Saga was changed concurrently.");
                    }
                    foreach (var claim in claims) { owner.reserved.Remove(claim); owner.inbox.Add(claim); }
                    claims.Clear();
                    owner.outbox.AddRange(outgoing);
                    owner.timeouts.AddRange(scheduled.Select(entry => (entry.Item1, entry.Item2)));
                    foreach (var record in recoveries) if (!owner.recovery.ContainsKey(record.RecoveryId)) owner.recovery.Add(record.RecoveryId, record);
                    foreach (var pair in changes)
                    {
                        if (pair.Value.Deleted) owner.sagas.Remove(pair.Key);
                        else owner.sagas[pair.Key] = new SagaRecord { Data = pair.Value.Data, Version = pair.Value.Version };
                    }
                    finished = true;
                }
            }

            public void Dispose()
            {
                if (finished) return;
                lock (owner.gate) foreach (var claim in claims) owner.reserved.Remove(claim);
                finished = true;
            }
        }
    }
}
