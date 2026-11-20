using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public interface IBehaviorContext : IExtendable { }

    public interface IIncomingContext : IBehaviorContext, IMessageProcessingContext { }

    public interface IIncomingLogicalMessageContext : IIncomingContext
    {
        Dictionary<string, string> Headers { get; }
        LogicalMessage Message { get; }
        bool MessageHandled { get; set; }
        void UpdateMessageInstance(object newInstance);
    }

    public sealed class LogicalMessage
    {
        internal LogicalMessage(Type type, object instance)
        {
            MessageType = type;
            Instance = instance;
        }

        public Type MessageType { get; }
        public object Instance { get; }
    }

    public interface IBehavior { }

    public interface IBehavior<in TInContext, out TOutContext> : IBehavior
        where TInContext : IBehaviorContext where TOutContext : IBehaviorContext
    {
        Task Invoke(TInContext context, Func<TOutContext, Task> next);
    }

    public abstract class Behavior<TContext> : IBehavior<TContext, TContext> where TContext : IBehaviorContext
    {
        public abstract Task Invoke(TContext context, Func<Task> next);

        public Task Invoke(TContext context, Func<TContext, Task> next)
        {
            if (next == null) throw new ArgumentNullException(nameof(next));
            return Invoke(context, () => next(context));
        }
    }

    public abstract class RegisterStep
    {
        internal List<(string Id, bool Before, bool Optional)> Dependencies { get; } =
            new List<(string Id, bool Before, bool Optional)>();

        protected RegisterStep(string stepId, Type behavior, string description)
            : this(stepId, behavior, description, null) { }

        protected RegisterStep(string stepId, Type behavior, string description, Func<IBehavior> factoryMethod)
        {
            if (string.IsNullOrWhiteSpace(stepId)) throw new ArgumentException("A step ID is required.", nameof(stepId));
            StepId = stepId;
            BehaviorType = behavior ?? throw new ArgumentNullException(nameof(behavior));
            Description = description;
            Factory = factoryMethod;
        }

        public string StepId { get; }
        public Type BehaviorType { get; }
        public string Description { get; }
        internal Func<IBehavior> Factory { get; }
        public void InsertBefore(string id) => AddDependency(id, before: true, optional: false);
        public void InsertBeforeIfExists(string id) => AddDependency(id, before: true, optional: true);
        public void InsertAfter(string id) => AddDependency(id, before: false, optional: false);
        public void InsertAfterIfExists(string id) => AddDependency(id, before: false, optional: true);

        void AddDependency(string id, bool before, bool optional)
        {
            if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("A referenced step ID is required.", nameof(id));
            Dependencies.Add((id, before, optional));
        }
    }

    public sealed class PipelineSettings
    {
        readonly IncomingPipeline incoming;
        readonly Dictionary<string, Func<IncomingMessageContext, Func<Task>, Task>> steps =
            new Dictionary<string, Func<IncomingMessageContext, Func<Task>, Task>>(StringComparer.Ordinal);
        readonly Dictionary<string, (string Id, bool Before, bool Optional)[]> ordering =
            new Dictionary<string, (string Id, bool Before, bool Optional)[]>(StringComparer.Ordinal);

        internal PipelineSettings(IncomingPipeline incoming) { this.incoming = incoming; }

        public void Register<TRegisterStep>() where TRegisterStep : RegisterStep, new() => Register(new TRegisterStep());

        public void Register(RegisterStep registration)
        {
            if (registration == null) throw new ArgumentNullException(nameof(registration));
            if (steps.ContainsKey(registration.StepId))
                throw new InvalidOperationException("A pipeline step already uses ID " + registration.StepId);
            var behavior = registration.Factory == null ? CreateBehavior(registration.BehaviorType) : registration.Factory();
            if (!registration.BehaviorType.IsInstanceOfType(behavior))
                throw new InvalidOperationException("The factory must return a behavior assignable to " + registration.BehaviorType.FullName + ".");
            Register(registration.StepId, behavior, registration.Description);
            ordering.Add(registration.StepId, registration.Dependencies.ToArray());
        }

        public void Register(Type behavior, string description) =>
            Register(TypeName(behavior), behavior, description);

        public void Register(string stepId, Type behavior, string description) =>
            Register(stepId, CreateBehavior(behavior), description);

        public void Register<T>(T behavior, string description) where T : IBehavior =>
            Register(TypeName(behavior?.GetType()), behavior, description);

        public void Register<T>(Func<T> factoryMethod, string description) where T : IBehavior =>
            Register(TypeName(typeof(T)), factoryMethod, description);

        public void Register<T>(string stepId, Func<T> factoryMethod, string description) where T : IBehavior
        {
            ValidateNewStepId(stepId);
            Register(stepId, CreateBehavior(factoryMethod), description);
        }

        public void Register<T>(string stepId, T behavior, string description) where T : IBehavior
        {
            ValidateNewStepId(stepId);
            var invoke = Adapt(behavior);
            steps.Add(stepId, invoke);
            incoming.Use(invoke);
        }

        public void Replace(string stepId, Type newBehavior, string description = null) =>
            Replace(stepId, CreateBehavior(newBehavior), description);

        public void Replace<T>(string stepId, Func<T> factoryMethod, string description = null) where T : IBehavior
        {
            FindStep(stepId);
            Replace(stepId, CreateBehavior(factoryMethod), description);
        }

        public void Replace<T>(string stepId, T newBehavior, string description = null) where T : IBehavior
        {
            var current = FindStep(stepId);
            var replacement = Adapt(newBehavior);
            incoming.Replace(current, replacement);
            steps[stepId] = replacement;
        }

        public void Remove(string stepId)
        {
            incoming.Remove(FindStep(stepId));
            steps.Remove(stepId);
            ordering.Remove(stepId);
        }

        internal IEnumerable<Func<IncomingMessageContext, Func<Task>, Task>> Order(
            IReadOnlyList<Func<IncomingMessageContext, Func<Task>, Task>> behaviors)
        {
            if (ordering.Count == 0) return behaviors;

            var predecessors = new Dictionary<Func<IncomingMessageContext, Func<Task>, Task>,
                List<Func<IncomingMessageContext, Func<Task>, Task>>>();
            foreach (var entry in ordering)
            {
                var step = steps[entry.Key];
                foreach (var dependency in entry.Value)
                {
                    if (!steps.TryGetValue(dependency.Id, out var other))
                    {
                        if (dependency.Optional) continue;
                        throw new InvalidOperationException("Pipeline step " + entry.Key + " references missing step " + dependency.Id);
                    }
                    var successor = dependency.Before ? other : step;
                    if (!predecessors.TryGetValue(successor, out var required))
                        predecessors.Add(successor, required = new List<Func<IncomingMessageContext, Func<Task>, Task>>());
                    required.Add(dependency.Before ? step : other);
                }
            }

            var sorted = new List<Func<IncomingMessageContext, Func<Task>, Task>>();
            var stepIds = steps.ToDictionary(entry => entry.Value, entry => entry.Key);
            var visiting = new HashSet<Func<IncomingMessageContext, Func<Task>, Task>>();
            var visited = new HashSet<Func<IncomingMessageContext, Func<Task>, Task>>();
            void Visit(Func<IncomingMessageContext, Func<Task>, Task> step)
            {
                if (visited.Contains(step)) return;
                if (!visiting.Add(step))
                    throw new InvalidOperationException("Pipeline step ordering contains a cycle involving " + stepIds[step] + ".");
                if (predecessors.TryGetValue(step, out var required))
                    foreach (var predecessor in required) Visit(predecessor);
                visiting.Remove(step);
                visited.Add(step);
                sorted.Add(step);
            }

            foreach (var behavior in behaviors)
            {
                if (stepIds.ContainsKey(behavior)) Visit(behavior);
                else sorted.Add(behavior);
            }
            return sorted;
        }

        Func<IncomingMessageContext, Func<Task>, Task> FindStep(string stepId)
        {
            if (string.IsNullOrWhiteSpace(stepId)) throw new ArgumentException("A step ID is required.", nameof(stepId));
            if (!steps.TryGetValue(stepId, out var invoke)) throw new InvalidOperationException("No pipeline step uses ID " + stepId);
            return invoke;
        }

        void ValidateNewStepId(string stepId)
        {
            if (string.IsNullOrWhiteSpace(stepId)) throw new ArgumentException("A step ID is required.", nameof(stepId));
            if (steps.ContainsKey(stepId)) throw new InvalidOperationException("A pipeline step already uses ID " + stepId);
        }

        static T CreateBehavior<T>(Func<T> factoryMethod) where T : IBehavior
        {
            if (factoryMethod == null) throw new ArgumentNullException(nameof(factoryMethod));
            var behavior = factoryMethod();
            if (behavior == null) throw new InvalidOperationException("The pipeline behavior factory returned null.");
            return behavior;
        }

        static IBehavior CreateBehavior(Type behavior)
        {
            if (behavior == null) throw new ArgumentNullException(nameof(behavior));
            if (behavior.IsAbstract || !typeof(IBehavior).IsAssignableFrom(behavior))
                throw new ArgumentException("A concrete pipeline behavior is required.", nameof(behavior));
            return (IBehavior)Activator.CreateInstance(behavior);
        }

        static Func<IncomingMessageContext, Func<Task>, Task> Adapt(IBehavior behavior)
        {
            if (behavior == null) throw new ArgumentNullException(nameof(behavior));
            Func<IncomingMessageContext, Func<Task>, Task> invoke;
            if (behavior is IBehavior<IncomingMessageContext, IncomingMessageContext> native)
                invoke = (context, next) => native.Invoke(context, continuation => Continue(context, continuation, next));
            else if (behavior is IBehavior<IIncomingLogicalMessageContext, IIncomingLogicalMessageContext> logical)
                invoke = (context, next) =>
                {
                    var view = new LogicalContext(context);
                    return logical.Invoke(view, continuation => Continue(view, continuation, next));
                };
            else
                throw new NotSupportedException("Register an incoming logical message behavior or a Bq incoming behavior.");
            return invoke;
        }

        static string TypeName(Type behavior) =>
            (behavior ?? throw new ArgumentNullException(nameof(behavior))).FullName;

        static Task Continue(object expected, object supplied, Func<Task> next)
        {
            if (!ReferenceEquals(expected, supplied)) throw new InvalidOperationException("The continuation must use the active incoming context.");
            return next();
        }

        sealed class LogicalContext : IIncomingLogicalMessageContext
        {
            readonly IncomingMessageContext context;

            public LogicalContext(IncomingMessageContext context)
            {
                this.context = context;
            }

            public LogicalMessage Message => new LogicalMessage(context.MessageType, context.Message);
            public Dictionary<string, string> Headers => context.LogicalHeaders;
            public bool MessageHandled
            {
                get => context.MessageHandled;
                set => context.MessageHandled = value;
            }
            public void UpdateMessageInstance(object newInstance) => context.UpdateMessageInstance(newInstance);
            public string MessageId => context.HandlerContext.MessageId;
            public string ReplyToAddress => context.HandlerContext.ReplyToAddress;
            public IReadOnlyDictionary<string, string> MessageHeaders => context.HandlerContext.MessageHeaders;
            public ContextBag Extensions => context.Extensions;
            public Task Send(object message, SendOptions options) => context.HandlerContext.Send(message, options);
            public Task Send<T>(Action<T> initialize, SendOptions options) => context.HandlerContext.Send(initialize, options);
            public Task Publish(object message, PublishOptions options) => context.HandlerContext.Publish(message, options);
            public Task Publish<T>(Action<T> initialize, PublishOptions options) => context.HandlerContext.Publish(initialize, options);
            public Task Reply(object message, ReplyOptions options) => context.HandlerContext.Reply(message, options);
            public Task Reply<T>(Action<T> initialize, ReplyOptions options) => context.HandlerContext.Reply(initialize, options);
            public Task ForwardCurrentMessageTo(string destination) => context.HandlerContext.ForwardCurrentMessageTo(destination);
        }
    }
}
