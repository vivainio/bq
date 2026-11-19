using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Bq.Messaging;

namespace Bq.Testing
{
    public sealed class RequestedTimeout
    {
        internal RequestedTimeout(DateTimeOffset dueAt, object state, Type stateType)
        {
            DueAt = dueAt;
            State = state;
            StateType = stateType;
        }

        public DateTimeOffset DueAt { get; }
        public object State { get; }
        public Type StateType { get; }
    }

    // Exercises a single saga instance. Use integration tests for correlation and persistence.
    public sealed class SagaTest<TSaga, TData> where TSaga : Saga<TData> where TData : class, new()
    {
        readonly List<RequestedTimeout> timeouts = new List<RequestedTimeout>();
        bool handling;

        public SagaTest(TSaga saga, TData data = null)
        {
            Saga = saga ?? throw new ArgumentNullException(nameof(saga));
            Saga.Initialize(data ?? new TData(), null);
        }

        public TSaga Saga { get; }
        public TData Data => Saga.Data;
        public bool Completed => Saga.Completed;
        public IReadOnlyList<RequestedTimeout> Timeouts => timeouts.AsReadOnly();

        public Task<TestableMessageHandlerContext> Handle<T>(T message, TestableMessageHandlerContext context = null) =>
            Invoke(c => ((IHandleMessages<T>)Saga).Handle(message, c), context);

        public Task<TestableMessageHandlerContext> Timeout<T>(T state, TestableMessageHandlerContext context = null) =>
            Invoke(c => ((IHandleTimeouts<T>)Saga).Timeout(state, c), context);

        async Task<TestableMessageHandlerContext> Invoke(Func<IMessageHandlerContext, Task> handle, TestableMessageHandlerContext context)
        {
            if (Completed || handling) throw new InvalidOperationException("The saga is complete or is already handling a message.");
            handling = true;
            context = context ?? new TestableMessageHandlerContext();
            Saga.Initialize(Data, (dueAt, state, stateType) =>
            {
                timeouts.Add(new RequestedTimeout(dueAt, state, stateType));
                return Task.CompletedTask;
            });
            try
            {
                await handle(context).ConfigureAwait(false);
                return context;
            }
            finally
            {
                Saga.Initialize(Data, null);
                handling = false;
            }
        }
    }
}
