using System;
using System.Threading.Tasks;
using MessagePack;
using MessagePack.Resolvers;

namespace Bq.Messaging
{
    // Explicit storage primitives; correlation and saga lifecycle are controlled by the handler.
    public sealed class SagaSnapshot<T>
    {
        public T Data { get; set; }
        public long Version { get; set; }
    }
    public static class SagaPersistenceExtensions
    {
        static readonly MessagePackSerializerOptions Options = MessagePackSerializerOptions.Standard
            .WithResolver(ContractlessStandardResolver.Instance).WithSecurity(MessagePackSecurity.UntrustedData);
        public static async Task<SagaSnapshot<T>> LoadSaga<T>(this IPersistenceSession session, string id)
        {
            var record = await session.LoadSaga(typeof(T).FullName, id).ConfigureAwait(false);
            return record == null ? null : new SagaSnapshot<T> { Data = MessagePackSerializer.Deserialize<T>(record.Data, Options), Version = record.Version };
        }
        public static Task SaveSaga<T>(this IPersistenceSession session, string id, T data, long? expectedVersion = null) =>
            session.SaveSaga(typeof(T).FullName, id, MessagePackSerializer.Serialize(data, Options), expectedVersion);
        public static Task DeleteSaga<T>(this IPersistenceSession session, string id, long expectedVersion) =>
            session.DeleteSaga(typeof(T).FullName, id, expectedVersion);
    }
}
