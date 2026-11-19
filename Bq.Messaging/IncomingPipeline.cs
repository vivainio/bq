using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public sealed class IncomingMessageContext
    {
        internal IncomingMessageContext(object message, IMessageHandlerContext handlerContext)
        {
            Message = message;
            HandlerContext = handlerContext;
        }

        public object Message { get; }
        public IMessageHandlerContext HandlerContext { get; }
        public IDictionary<string, object> Items { get; } = new Dictionary<string, object>();
    }

    public sealed class IncomingPipeline
    {
        readonly List<Func<IncomingMessageContext, Func<Task>, Task>> behaviors =
            new List<Func<IncomingMessageContext, Func<Task>, Task>>();
        readonly List<Func<IncomingMessageContext, IPersistenceSession, Task>> beforeCommit =
            new List<Func<IncomingMessageContext, IPersistenceSession, Task>>();

        public void Use(Func<IncomingMessageContext, Func<Task>, Task> behavior) =>
            behaviors.Add(behavior ?? throw new ArgumentNullException(nameof(behavior)));

        public void BeforeCommit(Func<IncomingMessageContext, IPersistenceSession, Task> callback) =>
            beforeCommit.Add(callback ?? throw new ArgumentNullException(nameof(callback)));

        internal Func<IncomingMessageContext, Func<Task>, Task> Build()
        {
            var callbacks = beforeCommit.ToArray();
            Func<IncomingMessageContext, Func<Task>, Task> pipeline = async (context, invokeHandler) =>
            {
                await invokeHandler().ConfigureAwait(false);
                foreach (var callback in callbacks)
                    await callback(context, context.HandlerContext.PersistenceSession).ConfigureAwait(false);
            };

            for (var index = behaviors.Count - 1; index >= 0; index--)
            {
                var behavior = behaviors[index];
                var next = pipeline;
                pipeline = (context, invokeHandler) => behavior(context, () => next(context, invokeHandler));
            }

            return pipeline;
        }
    }
}
