using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MessagePack;
using MessagePack.Resolvers;

namespace Bq.Messaging
{
    public interface IAmStartedByMessages<T> : IHandleMessages<T> { }
    public interface IHandleTimeouts<T>
    {
        Task Timeout(T state, IMessageHandlerContext context);
    }

    public interface IContainSagaData
    {
        Guid Id { get; set; }
        string Originator { get; set; }
        string OriginalMessageId { get; set; }
    }

    public abstract class ContainSagaData : IContainSagaData
    {
        public Guid Id { get; set; }
        public string Originator { get; set; }
        public string OriginalMessageId { get; set; }
    }

    // A new saga object is constructed for each message attempt.
    public abstract class Saga<TData> where TData : class, new()
    {
        Func<DateTimeOffset, object, Type, Task> requestTimeout;
        public TData Data { get; internal set; }
        internal bool Completed { get; private set; }
        internal void Initialize(TData data, Func<DateTimeOffset, object, Type, Task> request) { Data = data; requestTimeout = request; }
        protected virtual void ConfigureHowToFindSaga(SagaPropertyMapper<TData> mapper) { }

        protected SagaPropertyMapper<TData>.MessageMapping<TMessage> ConfigureMapping<TMessage>(
            System.Linq.Expressions.Expression<Func<TMessage, object>> property) where TMessage : class =>
            new SagaPropertyMapper<TData>.MessageMapping<TMessage>(property,
                (data, message) => (configuringMapper ?? throw new InvalidOperationException("Configure mappings inside ConfigureHowToFindSaga."))
                    .Add(data, message));

        SagaPropertyMapper<TData> configuringMapper;
        internal void ConfigureMappings(SagaPropertyMapper<TData> mapper)
        {
            configuringMapper = mapper;
            try { ConfigureHowToFindSaga(mapper); }
            finally { configuringMapper = null; }
        }

        protected Task RequestTimeout<T>(IMessageHandlerContext context, TimeSpan delay) where T : new() =>
            RequestTimeout(context, delay, new T());

        protected Task RequestTimeout<T>(IMessageHandlerContext context, DateTime dueAt) where T : new() =>
            RequestTimeout(context, dueAt, new T());

        protected Task RequestTimeout<T>(IMessageHandlerContext context, TimeSpan delay, T state) => RequestTimeout(delay, state);
        protected Task RequestTimeout<T>(IMessageHandlerContext context, DateTime utcDueAt, T state) =>
            RequestTimeout(new DateTimeOffset(utcDueAt), state);

        protected Task ReplyToOriginator(IMessageHandlerContext context, object message)
        {
            if (context == null) throw new ArgumentNullException(nameof(context));
            if (!(Data is IContainSagaData data) || string.IsNullOrEmpty(data.Originator))
                throw new InvalidOperationException("The saga has no recorded originator.");
            var options = new ReplyOptions { CorrelationId = data.OriginalMessageId };
            options.SetDestination(data.Originator);
            return context.Reply(message, options);
        }

        protected void MarkAsComplete() { Completed = true; }
        protected Task RequestTimeout<T>(TimeSpan delay, T state)
        {
            if (requestTimeout == null) throw new InvalidOperationException("Timeouts can only be requested while handling a saga message.");
            if (delay < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(delay));
            return RequestTimeout(DateTimeOffset.UtcNow.Add(delay), state);
        }

        protected Task RequestTimeout<T>(DateTimeOffset dueAt, T state)
        {
            if (requestTimeout == null) throw new InvalidOperationException("Timeouts can only be requested while handling a saga message.");
            if (state == null) throw new ArgumentNullException(nameof(state));
            return requestTimeout(dueAt, state, typeof(T));
        }
    }

    // Persisted as part of a timeout message. InstanceId distinguishes a restarted
    // saga with the same correlation key from the earlier, completed instance.
    public sealed class SagaTimeoutMessage
    {
        public string SagaType { get; set; }
        public string SagaKey { get; set; }
        public string InstanceId { get; set; }
        public string TimeoutType { get; set; }
        public byte[] State { get; set; }
    }
    public sealed class SagaStoredState<T>
    {
        public string InstanceId { get; set; }
        public T Data { get; set; }
    }

    public sealed class SagaMappings<TSaga, TData> where TSaga : Saga<TData> where TData : class, new()
    {
        internal readonly List<Action<EndpointConfiguration, SagaRuntime<TSaga, TData>>> Messages = new List<Action<EndpointConfiguration, SagaRuntime<TSaga, TData>>>();
        internal readonly Dictionary<string, Func<TSaga, object, IMessageHandlerContext, Task>> Timeouts = new Dictionary<string, Func<TSaga, object, IMessageHandlerContext, Task>>();
        public void Start<TMessage>(Func<TMessage, string> correlation, bool subscribe = false) where TMessage : class
        {
            if (!typeof(IAmStartedByMessages<TMessage>).IsAssignableFrom(typeof(TSaga))) throw new InvalidOperationException(typeof(TSaga).FullName + " must implement IAmStartedByMessages<" + typeof(TMessage).FullName + ">.");
            AddMessage(correlation, true, subscribe);
        }
        public void Handle<TMessage>(Func<TMessage, string> correlation, bool subscribe = false) where TMessage : class
        {
            if (!typeof(IHandleMessages<TMessage>).IsAssignableFrom(typeof(TSaga))) throw new InvalidOperationException(typeof(TSaga).FullName + " must implement IHandleMessages<" + typeof(TMessage).FullName + ">.");
            AddMessage(correlation, false, subscribe);
        }
        void AddMessage<TMessage>(Func<TMessage, string> correlation, bool start, bool subscribe) where TMessage : class
        {
            if (correlation == null) throw new ArgumentNullException(nameof(correlation));
            Messages.Add((configuration, runtime) =>
            {
                var name = EndpointConfiguration.MessageType(typeof(TMessage));
                configuration.AddRegistration(name, new Registration(typeof(TMessage), subscribe,
                    (message, context) => runtime.HandleMessage((TMessage)message, context, correlation, start)));
            });
        }
        internal void AddMappedMessage<TMessage>(Func<TMessage, IMessageHandlerContext, (string Key, Action<TData> Initialize)> correlation) where TMessage : class
        {
            if (!typeof(IHandleMessages<TMessage>).IsAssignableFrom(typeof(TSaga)))
                throw new InvalidOperationException(typeof(TSaga).FullName + " must handle " + typeof(TMessage).FullName);
            var start = typeof(IAmStartedByMessages<TMessage>).IsAssignableFrom(typeof(TSaga));
            Messages.Add((configuration, runtime) => configuration.AddRegistration(EndpointConfiguration.MessageType(typeof(TMessage)),
                new Registration(typeof(TMessage), false, (message, context) =>
                {
                    var typed = (TMessage)message;
                    var mapped = correlation(typed, context);
                    return runtime.HandleMappedMessage(typed, context, mapped.Key, start, mapped.Initialize);
                }, useEventConvention: true, subscribeByMarker: true)));
        }

        public void Timeout<TTimeout>() where TTimeout : class
        {
            if (!typeof(IHandleTimeouts<TTimeout>).IsAssignableFrom(typeof(TSaga))) throw new InvalidOperationException(typeof(TSaga).FullName + " must implement IHandleTimeouts<" + typeof(TTimeout).FullName + ">.");
            Timeouts.Add(EndpointConfiguration.MessageType(typeof(TTimeout)), (saga, state, context) => ((IHandleTimeouts<TTimeout>)saga).Timeout((TTimeout)state, context));
        }
    }

    public static class SagaConfigurationExtensions
    {
        public static void RegisterSaga<TSaga, TData>(this EndpointConfiguration endpoint)
            where TSaga : Saga<TData>, new() where TData : class, new() =>
            endpoint.RegisterSaga<TSaga, TData>(() => new TSaga());

        public static void RegisterSaga<TSaga, TData>(this EndpointConfiguration endpoint, Func<TSaga> factory)
            where TSaga : Saga<TData> where TData : class, new()
        {
            if (factory == null) throw new ArgumentNullException(nameof(factory));
            endpoint.RegisterSaga<TSaga, TData>(factory, mappings =>
            {
                var mapper = new SagaPropertyMapper<TData>();
                var definition = factory() ?? throw new InvalidOperationException("Saga factory returned null.");
                try { definition.ConfigureMappings(mapper); }
                finally { (definition as IDisposable)?.Dispose(); }
                mapper.Apply(mappings);
                var register = typeof(SagaMappings<TSaga, TData>).GetMethod(nameof(SagaMappings<TSaga, TData>.Timeout));
                foreach (var contract in typeof(TSaga).GetInterfaces())
                    if (contract.IsGenericType && contract.GetGenericTypeDefinition() == typeof(IHandleTimeouts<>))
                        register.MakeGenericMethod(contract.GetGenericArguments()[0]).Invoke(mappings, null);
            });
        }

        public static void RegisterSaga<TSaga, TData>(this EndpointConfiguration endpoint, Func<TSaga> factory, Action<SagaMappings<TSaga, TData>> configure)
            where TSaga : Saga<TData> where TData : class, new()
        {
            if (endpoint == null) throw new ArgumentNullException(nameof(endpoint));
            if (factory == null) throw new ArgumentNullException(nameof(factory));
            if (configure == null) throw new ArgumentNullException(nameof(configure));
            var mappings = new SagaMappings<TSaga, TData>();
            configure(mappings);
            if (mappings.Messages.Count == 0) throw new InvalidOperationException("A saga must handle at least one message.");
            var name = EndpointConfiguration.MessageType(typeof(TSaga));
            if (endpoint.SagaTimeouts.ContainsKey(name)) throw new InvalidOperationException("Saga is already registered: " + name);
            var runtime = new SagaRuntime<TSaga, TData>(factory, mappings.Timeouts, endpoint);
            foreach (var register in mappings.Messages) register(endpoint, runtime);
            endpoint.SagaTimeouts.Add(name, runtime.HandleTimeout);
            endpoint.HasSagas = true;
            var timeoutName = EndpointConfiguration.MessageType(typeof(SagaTimeoutMessage));
            if (!endpoint.Handlers.ContainsKey(timeoutName))
                endpoint.Handlers.Add(timeoutName, new Registration(typeof(SagaTimeoutMessage), false, (message, context) =>
                {
                    var timeout = (SagaTimeoutMessage)message;
                    if (timeout.SagaType == null || !endpoint.SagaTimeouts.TryGetValue(timeout.SagaType, out var handler))
                        throw new InvalidOperationException("No saga registered for timeout: " + timeout.SagaType);
                    return handler(timeout, context);
                }));
        }
    }

    internal sealed class SagaRuntime<TSaga, TData> where TSaga : Saga<TData> where TData : class, new()
    {
        static readonly MessagePackSerializerOptions Options = MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance).WithSecurity(MessagePackSecurity.UntrustedData);
        readonly string sagaType = EndpointConfiguration.MessageType(typeof(TSaga));
        readonly Func<TSaga> factory;
        readonly Dictionary<string, Func<TSaga, object, IMessageHandlerContext, Task>> timeouts;
        readonly EndpointConfiguration configuration;
        public SagaRuntime(Func<TSaga> factory, Dictionary<string, Func<TSaga, object, IMessageHandlerContext, Task>> timeouts, EndpointConfiguration configuration) { this.factory = factory; this.timeouts = timeouts; this.configuration = configuration; }
        public Task HandleMessage<TMessage>(TMessage message, IMessageHandlerContext context, Func<TMessage, string> correlation, bool canStart)
        {
            var key = correlation(message);
            if (string.IsNullOrEmpty(key)) throw new InvalidOperationException("Saga correlation key must not be empty.");
            return Execute(key, null, canStart, context, (saga) => ((IHandleMessages<TMessage>)saga).Handle(message, context), message);
        }
        public Task HandleMappedMessage<TMessage>(TMessage message, IMessageHandlerContext context, string key,
            bool canStart, Action<TData> initialize) =>
            Execute(key, null, canStart, context, saga => ((IHandleMessages<TMessage>)saga).Handle(message, context), message, initialize);

        public Task HandleTimeout(SagaTimeoutMessage timeout, IMessageHandlerContext context)
        {
            if (string.IsNullOrEmpty(timeout.SagaKey) || string.IsNullOrEmpty(timeout.InstanceId) || timeout.TimeoutType == null || timeout.State == null)
                throw new InvalidOperationException("Invalid saga timeout message.");
            if (!timeouts.TryGetValue(timeout.TimeoutType, out var invoke)) throw new InvalidOperationException("Saga timeout type is not registered: " + timeout.TimeoutType);
            // Only registered types can be deserialized. Resolve against the saga's interfaces.
            Type stateType = null;
            foreach (var item in typeof(TSaga).GetInterfaces())
                if (item.IsGenericType && item.GetGenericTypeDefinition() == typeof(IHandleTimeouts<>) && EndpointConfiguration.MessageType(item.GetGenericArguments()[0]) == timeout.TimeoutType) stateType = item.GetGenericArguments()[0];
            if (stateType == null) throw new InvalidOperationException("Saga timeout type cannot be resolved.");
            var state = MessagePackSerializer.Deserialize(stateType, timeout.State, Options);
            return Execute(timeout.SagaKey, timeout.InstanceId, false, context, saga => invoke(saga, state, context));
        }
        async Task Execute(string key, string expectedInstance, bool canStart, IMessageHandlerContext context, Func<TSaga, Task> invoke, object incomingMessage = null, Action<TData> initialize = null)
        {
            var session = context.PersistenceSession ?? throw new InvalidOperationException("Saga handling requires persistence.");
            var record = await session.LoadSaga(sagaType, key).ConfigureAwait(false);
            if (record == null && !canStart)
            {
                if (expectedInstance == null && configuration.SagaNotFound != null)
                    await configuration.SagaNotFound(typeof(TSaga), key, incomingMessage, context).ConfigureAwait(false);
                return;
            }
            var state = record == null ? new SagaStoredState<TData> { InstanceId = Guid.NewGuid().ToString("N"), Data = new TData() } : MessagePackSerializer.Deserialize<SagaStoredState<TData>>(record.Data, Options);
            if (expectedInstance != null && state.InstanceId != expectedInstance) return;
            if (record == null)
            {
                initialize?.Invoke(state.Data);
                if (state.Data is IContainSagaData data)
                {
                    data.Id = Guid.ParseExact(state.InstanceId, "N");
                    data.Originator = context.ReplyToAddress;
                    data.OriginalMessageId = context.MessageId;
                }
            }
            var saga = factory() ?? throw new InvalidOperationException("Saga factory returned null.");
            try
            {
                saga.Initialize(state.Data, (dueAt, timeoutState, timeoutType) =>
                {
                    var typeName = EndpointConfiguration.MessageType(timeoutType);
                    if (!timeouts.ContainsKey(typeName)) throw new InvalidOperationException("Saga timeout type is not registered: " + typeName);
                    var timeout = new SagaTimeoutMessage { SagaType = sagaType, SagaKey = key, InstanceId = state.InstanceId, TimeoutType = typeName, State = MessagePackSerializer.Serialize(timeoutType, timeoutState, Options) };
                    return ((RabbitEndpoint.HandlerContext)context).ScheduleSagaTimeout(timeout, dueAt.UtcDateTime);
                });
                await invoke(saga).ConfigureAwait(false);
                if (saga.Completed)
                {
                    if (record != null) await session.DeleteSaga(sagaType, key, record.Version).ConfigureAwait(false);
                }
                else await session.SaveSaga(sagaType, key, MessagePackSerializer.Serialize(state, Options), record?.Version).ConfigureAwait(false);
            }
            finally { (saga as IDisposable)?.Dispose(); }
        }
    }
}
