using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Bq.Messaging.Outbox
{
    public interface OutboxTransaction : IDisposable
    {
        Task Commit();
    }
}

namespace Bq.Messaging.Transport
{
    public class TransportTransaction : ContextBag { }
}

namespace Bq.Messaging.Persistence
{
    using Bq.Messaging.Outbox;
    using Bq.Messaging.Transport;

    public interface CompletableSynchronizedStorageSession : SynchronizedStorageSession, IDisposable
    {
        Task CompleteAsync();
    }

    public interface ISynchronizedStorage
    {
        Task<CompletableSynchronizedStorageSession> OpenSession(ContextBag contextBag);
    }

    public interface ISynchronizedStorageAdapter
    {
        Task<CompletableSynchronizedStorageSession> TryAdapt(OutboxTransaction transaction, ContextBag context);
        Task<CompletableSynchronizedStorageSession> TryAdapt(TransportTransaction transaction, ContextBag context);
    }

    public class CompletableSynchronizedStorageSessionAdapter : CompletableSynchronizedStorageSession
    {
        readonly ISynchronizedStorageAdapter adapter;
        readonly ISynchronizedStorage synchronizedStorage;

        public CompletableSynchronizedStorageSessionAdapter(ISynchronizedStorageAdapter adapter, ISynchronizedStorage synchronizedStorage)
        {
            this.adapter = adapter;
            this.synchronizedStorage = synchronizedStorage;
        }

        public CompletableSynchronizedStorageSession AdaptedSession { get; protected set; }
        internal void SetAdapted(CompletableSynchronizedStorageSession session) => AdaptedSession = session;
        public DbConnection Connection => AdaptedSession?.Connection;
        public DbTransaction Transaction => AdaptedSession?.Transaction;

        public async Task Open(ContextBag contextBag) => AdaptedSession = await synchronizedStorage.OpenSession(contextBag).ConfigureAwait(false);

        public async Task<bool> TryOpen(OutboxTransaction transaction, ContextBag context)
        {
            AdaptedSession = adapter == null ? null : await adapter.TryAdapt(transaction, context).ConfigureAwait(false);
            return AdaptedSession != null;
        }

        public async Task<bool> TryOpen(TransportTransaction transportTransaction, ContextBag context)
        {
            AdaptedSession = adapter == null ? null : await adapter.TryAdapt(transportTransaction, context).ConfigureAwait(false);
            return AdaptedSession != null;
        }

        public Task CompleteAsync() => AdaptedSession?.CompleteAsync() ?? Task.CompletedTask;
        public void Dispose() => AdaptedSession?.Dispose();
    }

    public static class CompletableSynchronizedStorageSessionAdapterExtensions
    {
        /// <summary>Adapts the session of the message attempt that is running. Bq commits that session itself, so completing it does nothing.</summary>
        public static Task Open(this CompletableSynchronizedStorageSessionAdapter adapter, IIncomingLogicalMessageContext context)
        {
            if (adapter == null) throw new ArgumentNullException(nameof(adapter));
            if (context == null) throw new ArgumentNullException(nameof(context));
            var session = RabbitEndpoint.CurrentPersistenceSession();
            return adapter.Open(new AttemptSession(session), context.Extensions);
        }

        public static Task Open(this CompletableSynchronizedStorageSessionAdapter adapter, OutboxTransaction outboxTransaction,
            TransportTransaction transportTransaction, ContextBag context) =>
            adapter.TryOpen(outboxTransaction, context).ContinueWith(task =>
                task.Result ? Task.FromResult(true) : adapter.TryOpen(transportTransaction, context)).Unwrap();

        static Task Open(this CompletableSynchronizedStorageSessionAdapter adapter, CompletableSynchronizedStorageSession session, ContextBag context)
        {
            adapter.SetAdapted(session);
            return Task.CompletedTask;
        }

        sealed class AttemptSession : CompletableSynchronizedStorageSession
        {
            readonly SynchronizedStorageSession session;
            public AttemptSession(SynchronizedStorageSession session) { this.session = session; }
            public DbConnection Connection => session.Connection;
            public DbTransaction Transaction => session.Transaction;
            public Task CompleteAsync() => Task.CompletedTask;
            public void Dispose() { }
        }
    }

    /// <summary>Exposes a Bq persistence provider through the NServiceBus synchronized storage contracts.</summary>
    public sealed class PersistenceSynchronizedStorage : ISynchronizedStorage, ISynchronizedStorageAdapter
    {
        readonly IPersistenceProvider provider;

        public PersistenceSynchronizedStorage(IPersistenceProvider provider) { this.provider = provider ?? throw new ArgumentNullException(nameof(provider)); }

        public async Task<CompletableSynchronizedStorageSession> OpenSession(ContextBag contextBag) =>
            new OwnedSession(await provider.OpenSession().ConfigureAwait(false));

        // Bq has no separate outbox or transport transactions to adapt: a message attempt owns one database transaction.
        public Task<CompletableSynchronizedStorageSession> TryAdapt(OutboxTransaction transaction, ContextBag context) =>
            Task.FromResult<CompletableSynchronizedStorageSession>(null);

        public Task<CompletableSynchronizedStorageSession> TryAdapt(TransportTransaction transaction, ContextBag context) =>
            Task.FromResult<CompletableSynchronizedStorageSession>(null);

        sealed class OwnedSession : CompletableSynchronizedStorageSession
        {
            readonly IPersistenceSession session;
            public OwnedSession(IPersistenceSession session) { this.session = session; }
            public DbConnection Connection => session.Connection;
            public DbTransaction Transaction => session.Transaction;
            public Task CompleteAsync() { session.Commit(); return Task.CompletedTask; }
            public void Dispose() => session.Dispose();
        }
    }
}
