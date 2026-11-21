using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Bq.Messaging.Sagas
{
    public class SagaCorrelationProperty
    {
        public SagaCorrelationProperty(string name, Type type)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Type = type ?? throw new ArgumentNullException(nameof(type));
        }

        public string Name { get; }
        public Type Type { get; }
    }

    public class CorrelationPropertyMetadata
    {
        public CorrelationPropertyMetadata(string name, Type type)
        {
            Name = name;
            Type = type;
        }

        public string Name { get; }
        public Type Type { get; }
    }

    public class SagaMessage
    {
        public SagaMessage(Type messageType, bool isAllowedToStartSaga)
        {
            MessageType = messageType?.FullName ?? throw new ArgumentNullException(nameof(messageType));
            IsAllowedToStartSaga = isAllowedToStartSaga;
        }

        public string MessageType { get; }
        public bool IsAllowedToStartSaga { get; }
    }

    public class SagaFinderDefinition
    {
        public SagaFinderDefinition(Type type, Type messageType, IDictionary<string, object> properties)
        {
            Type = type;
            this.messageType = messageType;
            Properties = properties ?? new Dictionary<string, object>();
        }

        public Type Type { get; }
        public string MessageType => messageType.FullName;
        public IDictionary<string, object> Properties { get; }
        readonly Type messageType;
    }

    /// <summary>Marker for custom saga finders. Bq finds sagas by their single correlation key, so it does not call finders.</summary>
    public interface IFinder { }

    public interface IFindSagas<TSagaData> where TSagaData : class, IContainSagaData
    {
        public interface Using<TMessage> : IFinder
        {
            Task<TSagaData> FindBy(TMessage message, SynchronizedStorageSession storageSession, ReadOnlyContextBag context);
        }
    }

    public class SagaIdGeneratorContext
    {
        public SagaIdGeneratorContext(string correlationPropertyName, object correlationPropertyValue, SagaMetadata sagaMetadata, ContextBag context)
        {
            CorrelationProperty = new SagaCorrelationProperty(correlationPropertyName ?? "", correlationPropertyValue?.GetType() ?? typeof(object));
            CorrelationPropertyValue = correlationPropertyValue;
            SagaMetadata = sagaMetadata;
            Context = context;
        }

        public SagaCorrelationProperty CorrelationProperty { get; }
        public object CorrelationPropertyValue { get; }
        public SagaMetadata SagaMetadata { get; }
        public ContextBag Context { get; }
    }

    public interface ISagaIdGenerator
    {
        Guid Generate(SagaIdGeneratorContext context);
    }

    /// <summary>NServiceBus's saga persister contract. Bq stores sagas through <see cref="IPersistenceSession"/>; use <see cref="PersistenceSagaPersister"/> to reach it.</summary>
    public interface ISagaPersister
    {
        Task Save(IContainSagaData sagaData, SagaCorrelationProperty correlationProperty, SynchronizedStorageSession session, ContextBag context);
        Task Update(IContainSagaData sagaData, SynchronizedStorageSession session, ContextBag context);
        Task<TSagaData> Get<TSagaData>(Guid sagaId, SynchronizedStorageSession session, ContextBag context) where TSagaData : class, IContainSagaData;
        Task<TSagaData> Get<TSagaData>(string propertyName, object propertyValue, SynchronizedStorageSession session, ContextBag context) where TSagaData : class, IContainSagaData;
        Task Complete(IContainSagaData sagaData, SynchronizedStorageSession session, ContextBag context);
    }

    public class SagaMetadata
    {
        SagaMetadata(string name, Type sagaType, string entityName, Type sagaEntityType, CorrelationPropertyMetadata correlation,
            IReadOnlyList<SagaMessage> messages, IReadOnlyList<SagaFinderDefinition> finders)
        {
            Name = name;
            SagaType = sagaType;
            EntityName = entityName;
            SagaEntityType = sagaEntityType;
            CorrelationProperty = correlation;
            AssociatedMessages = messages;
            Finders = finders;
        }

        public string Name { get; }
        public string EntityName { get; }
        public Type SagaEntityType { get; }
        public Type SagaType { get; }
        public CorrelationPropertyMetadata CorrelationProperty { get; }
        public IReadOnlyList<SagaMessage> AssociatedMessages { get; }
        public IReadOnlyList<SagaFinderDefinition> Finders { get; }

        public bool IsMessageAllowedToStartTheSaga(string messageType) =>
            AssociatedMessages.Any(message => message.MessageType == messageType && message.IsAllowedToStartSaga);

        public static bool IsSagaType(Type type) => SagaDataOf(type) != null && !type.IsAbstract;

        public static SagaMetadata Create(Type sagaType)
        {
            if (sagaType == null) throw new ArgumentNullException(nameof(sagaType));
            var dataType = SagaDataOf(sagaType) ?? throw new ArgumentException(sagaType.FullName + " is not a saga.", nameof(sagaType));
            var interfaces = sagaType.GetInterfaces();
            var messages = new List<SagaMessage>();
            foreach (var contract in interfaces.Where(i => i.IsGenericType))
            {
                var definition = contract.GetGenericTypeDefinition();
                var message = contract.GetGenericArguments()[0];
                if (definition == typeof(IAmStartedByMessages<>)) messages.Add(new SagaMessage(message, true));
                else if (definition == typeof(IHandleMessages<>) && !messages.Any(m => m.MessageType == message.FullName))
                    messages.Add(new SagaMessage(message, false));
                else if (definition == typeof(IHandleTimeouts<>)) messages.Add(new SagaMessage(message, false));
            }
            var correlation = (CorrelationPropertyMetadata)typeof(SagaMetadata).GetMethod(nameof(Correlation), BindingFlags.Static | BindingFlags.NonPublic)
                .MakeGenericMethod(dataType).Invoke(null, new object[] { sagaType });
            return new SagaMetadata(sagaType.FullName, sagaType, dataType.FullName, dataType, correlation, messages, new SagaFinderDefinition[0]);
        }

        static Type SagaDataOf(Type type)
        {
            for (var current = type; current != null; current = current.BaseType)
                if (current.IsGenericType && current.GetGenericTypeDefinition() == typeof(Saga<>)) return current.GetGenericArguments()[0];
            return null;
        }

        static CorrelationPropertyMetadata Correlation<TData>(Type sagaType) where TData : class, new()
        {
            var definition = (Saga<TData>)System.Runtime.Serialization.FormatterServices.GetUninitializedObject(sagaType);
            var mapper = new SagaPropertyMapper<TData>();
            definition.ConfigureMappings(mapper);
            var property = mapper.CorrelationProperty;
            return property == null ? null : new CorrelationPropertyMetadata(property.Name, property.PropertyType);
        }
    }

    public class SagaMetadataCollection : IEnumerable<SagaMetadata>
    {
        readonly List<SagaMetadata> metadata = new List<SagaMetadata>();

        public SagaMetadataCollection() { }

        public SagaMetadataCollection(IEnumerable<Type> sagaTypes) => Initialize(sagaTypes);

        public void Initialize(IEnumerable<Type> availableTypes) =>
            metadata.AddRange((availableTypes ?? throw new ArgumentNullException(nameof(availableTypes))).Where(SagaMetadata.IsSagaType).Select(SagaMetadata.Create));

        public SagaMetadata Find(Type sagaType) => metadata.FirstOrDefault(m => m.SagaType == sagaType) ??
            throw new KeyNotFoundException("The saga is not registered: " + sagaType?.FullName);

        public SagaMetadata FindByEntity(string entityName) => metadata.FirstOrDefault(m => m.EntityName == entityName) ??
            throw new KeyNotFoundException("No saga uses the entity " + entityName);

        public System.Collections.Generic.IEnumerator<SagaMetadata> GetEnumerator() => metadata.GetEnumerator();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>Gives <see cref="ISagaPersister"/> callers access to Bq's saga store, keyed by saga type and correlation value.</summary>
    public sealed class PersistenceSagaPersister : ISagaPersister
    {
        public Task Save(IContainSagaData sagaData, SagaCorrelationProperty correlationProperty, SynchronizedStorageSession session, ContextBag context) =>
            throw new NotSupportedException("Bq creates saga rows itself, from the correlation key of the message that starts the saga.");

        public Task Update(IContainSagaData sagaData, SynchronizedStorageSession session, ContextBag context) =>
            throw new NotSupportedException("Bq updates sagas inside the message attempt that handles them.");

        public Task Complete(IContainSagaData sagaData, SynchronizedStorageSession session, ContextBag context) =>
            throw new NotSupportedException("Call MarkAsComplete inside the saga instead.");

        public Task<TSagaData> Get<TSagaData>(Guid sagaId, SynchronizedStorageSession session, ContextBag context) where TSagaData : class, IContainSagaData =>
            throw new NotSupportedException("Bq looks up sagas by correlation value, not by saga ID.");

        public async Task<TSagaData> Get<TSagaData>(string propertyName, object propertyValue, SynchronizedStorageSession session, ContextBag context)
            where TSagaData : class, IContainSagaData
        {
            if (!(session is IPersistenceSession persistence))
                throw new ArgumentException("A Bq persistence session is required.", nameof(session));
            var record = await persistence.LoadSaga(typeof(TSagaData).FullName,
                Convert.ToString(propertyValue, System.Globalization.CultureInfo.InvariantCulture)).ConfigureAwait(false);
            return record == null ? null : MessagePack.MessagePackSerializer.Deserialize<SagaStoredState<TSagaData>>(record.Data,
                MessagePack.MessagePackSerializerOptions.Standard.WithResolver(MessagePack.Resolvers.ContractlessStandardResolver.Instance)).Data;
        }
    }
}
