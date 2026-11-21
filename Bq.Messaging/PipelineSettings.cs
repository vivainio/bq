using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public interface IBehaviorContext : IExtendable
    {
        Bq.Messaging.ObjectBuilder.IBuilder Builder { get; }
    }

    public interface IIncomingContext : IBehaviorContext, IMessageProcessingContext { }

    public interface IIncomingLogicalMessageContext : IIncomingContext
    {
        Dictionary<string, string> Headers { get; }
        LogicalMessage Message { get; }
        bool MessageHandled { get; set; }
        void UpdateMessageInstance(object newInstance);
    }

    public interface IIncomingPhysicalMessageContext : IIncomingContext
    {
        IncomingMessage Message { get; }
        void UpdateMessage(byte[] body);
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
        readonly Dictionary<string, Func<IncomingPhysicalContext, Func<Task>, Task>> physicalSteps =
            new Dictionary<string, Func<IncomingPhysicalContext, Func<Task>, Task>>(StringComparer.Ordinal);
        readonly Dictionary<string, Func<InvokeHandlerContext, Func<Task>, Task>> invokeHandlerSteps =
            new Dictionary<string, Func<InvokeHandlerContext, Func<Task>, Task>>(StringComparer.Ordinal);
        readonly Dictionary<OutgoingStage, Dictionary<string, Func<OutgoingContext, Func<Task>, Task>>> outgoingSteps =
            new Dictionary<OutgoingStage, Dictionary<string, Func<OutgoingContext, Func<Task>, Task>>>();
        readonly Dictionary<string, (string Id, bool Before, bool Optional)[]> ordering =
            new Dictionary<string, (string Id, bool Before, bool Optional)[]>(StringComparer.Ordinal);

        internal PipelineSettings(IncomingPipeline incoming) { this.incoming = incoming; }

        public void Register<TRegisterStep>() where TRegisterStep : RegisterStep, new() => Register(new TRegisterStep());

        public void Register(RegisterStep registration)
        {
            if (registration == null) throw new ArgumentNullException(nameof(registration));
            if (steps.ContainsKey(registration.StepId) || physicalSteps.ContainsKey(registration.StepId) ||
                TryFindOutgoing(registration.StepId, out _, out _))
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
            if (behavior is IBehavior<IInvokeHandlerContext, IInvokeHandlerContext> invokeHandler)
            {
                Func<InvokeHandlerContext, Func<Task>, Task> adapted = (c, next) => invokeHandler.Invoke(c, x => Continue(c, x, next));
                invokeHandlerSteps.Add(stepId, adapted);
                incoming.UseInvokeHandler(adapted);
                return;
            }
            if (TryAdaptOutgoing(behavior, out var outgoingStage, out var outgoingInvoke))
            {
                OutgoingSteps(outgoingStage).Add(stepId, outgoingInvoke);
                incoming.UseOutgoing(outgoingStage, outgoingInvoke);
                return;
            }
            if (IsPhysical(behavior))
            {
                var physical = AdaptPhysical(behavior);
                physicalSteps.Add(stepId, physical);
                incoming.UsePhysical(physical);
                return;
            }
            var invoke = Adapt(behavior);
            steps.Add(stepId, invoke);
            incoming.Use(invoke);
        }

        public void Replace(string stepId, Type newBehavior, string description = null) =>
            Replace(stepId, CreateBehavior(newBehavior), description);

        public void Replace<T>(string stepId, Func<T> factoryMethod, string description = null) where T : IBehavior
        {
            RequireStep(stepId);
            Replace(stepId, CreateBehavior(factoryMethod), description);
        }

        public void Replace<T>(string stepId, T newBehavior, string description = null) where T : IBehavior
        {
            if (invokeHandlerSteps.TryGetValue(stepId ?? string.Empty, out var currentInvoke))
            {
                if (!(newBehavior is IBehavior<IInvokeHandlerContext, IInvokeHandlerContext> replacementInvoke))
                    throw new InvalidOperationException("Pipeline step " + stepId + " must be replaced by a behavior for the same stage.");
                Func<InvokeHandlerContext, Func<Task>, Task> adaptedReplacement = (c, next) => replacementInvoke.Invoke(c, x => Continue(c, x, next));
                incoming.ReplaceInvokeHandler(currentInvoke, adaptedReplacement);
                invokeHandlerSteps[stepId] = adaptedReplacement;
                return;
            }
            if (TryFindOutgoing(stepId, out var existingStage, out var existingOutgoing))
            {
                if (!TryAdaptOutgoing(newBehavior, out var newStage, out var newInvoke) || newStage != existingStage)
                    throw new InvalidOperationException("Pipeline step " + stepId + " must be replaced by a behavior for the same stage.");
                incoming.ReplaceOutgoing(existingStage, existingOutgoing, newInvoke);
                OutgoingSteps(existingStage)[stepId] = newInvoke;
                return;
            }
            if (physicalSteps.TryGetValue(stepId ?? string.Empty, out var currentPhysical))
            {
                if (!IsPhysical(newBehavior))
                    throw new InvalidOperationException("Pipeline step " + stepId + " must be replaced by a behavior for the same stage.");
                var physicalReplacement = AdaptPhysical(newBehavior);
                incoming.ReplacePhysical(currentPhysical, physicalReplacement);
                physicalSteps[stepId] = physicalReplacement;
                return;
            }
            if (IsPhysical(newBehavior) || TryAdaptOutgoing(newBehavior, out _, out _))
            {
                FindStep(stepId);
                throw new InvalidOperationException("Pipeline step " + stepId + " must be replaced by a behavior for the same stage.");
            }
            var current = FindStep(stepId);
            var replacement = Adapt(newBehavior);
            incoming.Replace(current, replacement);
            steps[stepId] = replacement;
        }

        public void Remove(string stepId)
        {
            if (invokeHandlerSteps.TryGetValue(stepId ?? string.Empty, out var removeInvoke))
            {
                incoming.RemoveInvokeHandler(removeInvoke);
                invokeHandlerSteps.Remove(stepId);
            }
            else if (TryFindOutgoing(stepId, out var removeStage, out var removeOutgoing))
            {
                incoming.RemoveOutgoing(removeStage, removeOutgoing);
                OutgoingSteps(removeStage).Remove(stepId);
            }
            else if (physicalSteps.TryGetValue(stepId ?? string.Empty, out var physical))
            {
                incoming.RemovePhysical(physical);
                physicalSteps.Remove(stepId);
            }
            else
            {
                incoming.Remove(FindStep(stepId));
                steps.Remove(stepId);
            }
            ordering.Remove(stepId);
        }

        void RequireStep(string stepId)
        {
            if (string.IsNullOrWhiteSpace(stepId)) throw new ArgumentException("A step ID is required.", nameof(stepId));
            if (!steps.ContainsKey(stepId) && !physicalSteps.ContainsKey(stepId) && !invokeHandlerSteps.ContainsKey(stepId) && !TryFindOutgoing(stepId, out _, out _))
                throw new InvalidOperationException("No pipeline step uses ID " + stepId);
        }

        internal IEnumerable<Func<IncomingMessageContext, Func<Task>, Task>> Order(
            IReadOnlyList<Func<IncomingMessageContext, Func<Task>, Task>> behaviors) => Order(steps, behaviors);

        internal IEnumerable<Func<IncomingPhysicalContext, Func<Task>, Task>> OrderPhysical(
            IReadOnlyList<Func<IncomingPhysicalContext, Func<Task>, Task>> behaviors) => Order(physicalSteps, behaviors);

        IEnumerable<T> Order<T>(Dictionary<string, T> stageSteps, IReadOnlyList<T> behaviors) where T : class
        {
            if (stageSteps.Count == 0) return behaviors;

            var predecessors = new Dictionary<T, List<T>>();
            foreach (var entry in ordering)
            {
                if (!stageSteps.TryGetValue(entry.Key, out var step)) continue;
                foreach (var dependency in entry.Value)
                {
                    if (!stageSteps.TryGetValue(dependency.Id, out var other))
                    {
                        if (dependency.Optional) continue;
                        throw new InvalidOperationException("Pipeline step " + entry.Key + " references missing step " + dependency.Id);
                    }
                    var successor = dependency.Before ? other : step;
                    if (!predecessors.TryGetValue(successor, out var required))
                        predecessors.Add(successor, required = new List<T>());
                    required.Add(dependency.Before ? step : other);
                }
            }

            var sorted = new List<T>();
            var stepIds = stageSteps.ToDictionary(entry => entry.Value, entry => entry.Key);
            var visiting = new HashSet<T>();
            var visited = new HashSet<T>();
            void Visit(T step)
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
            if (steps.ContainsKey(stepId) || physicalSteps.ContainsKey(stepId) || invokeHandlerSteps.ContainsKey(stepId) || TryFindOutgoing(stepId, out _, out _))
                throw new InvalidOperationException("A pipeline step already uses ID " + stepId);
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

        Dictionary<string, Func<OutgoingContext, Func<Task>, Task>> OutgoingSteps(OutgoingStage stage)
        {
            if (!outgoingSteps.TryGetValue(stage, out var stageSteps))
                outgoingSteps.Add(stage, stageSteps = new Dictionary<string, Func<OutgoingContext, Func<Task>, Task>>(StringComparer.Ordinal));
            return stageSteps;
        }

        bool TryFindOutgoing(string stepId, out OutgoingStage stage, out Func<OutgoingContext, Func<Task>, Task> invoke)
        {
            foreach (var pair in outgoingSteps)
                if (stepId != null && pair.Value.TryGetValue(stepId, out invoke)) { stage = pair.Key; return true; }
            stage = default(OutgoingStage);
            invoke = null;
            return false;
        }

        internal IEnumerable<Func<InvokeHandlerContext, Func<Task>, Task>> OrderInvokeHandler(
            IReadOnlyList<Func<InvokeHandlerContext, Func<Task>, Task>> behaviors) => Order(invokeHandlerSteps, behaviors);

        internal IEnumerable<Func<OutgoingContext, Func<Task>, Task>> OrderOutgoing(OutgoingStage stage,
            IReadOnlyList<Func<OutgoingContext, Func<Task>, Task>> behaviors) => Order(OutgoingSteps(stage), behaviors);

        static bool TryAdaptOutgoing(IBehavior behavior, out OutgoingStage stage, out Func<OutgoingContext, Func<Task>, Task> invoke)
        {
            switch (behavior)
            {
                case IBehavior<IOutgoingSendContext, IOutgoingSendContext> send:
                    stage = OutgoingStage.Send; invoke = (c, next) => send.Invoke(c, x => Continue(c, x, next)); return true;
                case IBehavior<IOutgoingPublishContext, IOutgoingPublishContext> publish:
                    stage = OutgoingStage.Publish; invoke = (c, next) => publish.Invoke(c, x => Continue(c, x, next)); return true;
                case IBehavior<IOutgoingReplyContext, IOutgoingReplyContext> reply:
                    stage = OutgoingStage.Reply; invoke = (c, next) => reply.Invoke(c, x => Continue(c, x, next)); return true;
                case IBehavior<IOutgoingLogicalMessageContext, IOutgoingLogicalMessageContext> logical:
                    stage = OutgoingStage.Logical; invoke = (c, next) => logical.Invoke(c, x => Continue(c, x, next)); return true;
                case IBehavior<IOutgoingPhysicalMessageContext, IOutgoingPhysicalMessageContext> physical:
                    stage = OutgoingStage.Physical; invoke = (c, next) => physical.Invoke(c, x => Continue(c, x, next)); return true;
            }
            stage = default(OutgoingStage);
            invoke = null;
            return false;
        }

        static bool IsPhysical(IBehavior behavior) =>
            behavior is IBehavior<IIncomingPhysicalMessageContext, IIncomingPhysicalMessageContext>;

        static Func<IncomingPhysicalContext, Func<Task>, Task> AdaptPhysical(IBehavior behavior)
        {
            var physical = (IBehavior<IIncomingPhysicalMessageContext, IIncomingPhysicalMessageContext>)behavior;
            return (context, next) => physical.Invoke(context, continuation => Continue(context, continuation, next));
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
                throw new NotSupportedException("Register an incoming physical or logical message behavior or a Bq incoming behavior.");
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
            public Bq.Messaging.ObjectBuilder.IBuilder Builder => context.Builder;
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
