using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Bq.Messaging.Configuration.AdvancedExtensibility;

namespace Bq.Messaging
{
    /// <summary>Bq always runs the transactional outbox. The settings object exists so NServiceBus configuration compiles.</summary>
    public class OutboxSettings : ExposeSettings
    {
        internal OutboxSettings(EndpointConfiguration configuration) : base(configuration.GetSettings()) { }

        public OutboxSettings TimeToKeepDeduplicationData(TimeSpan time) =>
            throw new NotSupportedException("Bq does not clean deduplication data automatically. Remove expired inbox rows from the host.");

        public OutboxSettings NoWaitOnOutboxTimeouts() => this;
    }

    public class UnitOfWorkSettings
    {
        public UnitOfWorkSettings WrapHandlersInATransactionScope(TimeSpan? timeout = null, System.Transactions.IsolationLevel? isolationLevel = null) =>
            throw new NotSupportedException("Bq shares the message's database transaction through PersistenceSession, ITransactionalSession and " +
                "Incoming.BeforeCommit. A TransactionScope would enlist a second resource manager and escalate to MSDTC.");
    }

    /// <summary>Host identity for diagnostics. Bq records the values but does not add host headers to messages.</summary>
    public class HostInfoSettings
    {
        internal Guid? HostId { get; private set; }
        internal string DisplayName { get; private set; }

        public HostInfoSettings UsingCustomIdentifier(Guid id) { HostId = id; return this; }
        public HostInfoSettings UsingCustomDisplayName(string displayName) { DisplayName = displayName; return this; }
        public HostInfoSettings UsingNames(string instanceName, string hostName) { DisplayName = instanceName; return this; }
        public HostInfoSettings UsingInstalledFilePath() => this;
    }

    public class ScheduledTask : IMessage
    {
        public Guid TaskId { get; set; }
        public string Name { get; set; }
        public TimeSpan Every { get; set; }
    }

    public static class ScheduleExtensions
    {
        public static Task ScheduleEvery(this IMessageSession session, TimeSpan timeSpan, Func<IPipelineContext, Task> task) =>
            session.ScheduleEvery(timeSpan, task.Method.Name, task);

        public static Task ScheduleEvery(this IMessageSession session, TimeSpan timeSpan, string name, Func<IPipelineContext, Task> task)
        {
            if (session == null) throw new ArgumentNullException(nameof(session));
            if (task == null) throw new ArgumentNullException(nameof(task));
            if (timeSpan <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(timeSpan));
            var endpoint = session as RabbitEndpoint ?? throw new NotSupportedException("Scheduling requires an endpoint started by Bq.");
            var scheduled = new ScheduledTask { TaskId = Guid.NewGuid(), Name = name, Every = timeSpan };
            endpoint.Configuration.ScheduledTasks[scheduled.TaskId] = task;
            return endpoint.SendLocalAt(scheduled, DateTimeOffset.UtcNow + timeSpan);
        }
    }

    public sealed partial class EndpointConfiguration
    {
        internal readonly ConcurrentDictionary<Guid, Func<IPipelineContext, Task>> ScheduledTasks =
            new ConcurrentDictionary<Guid, Func<IPipelineContext, Task>>();
        internal List<Type> HandlerOrder { get; } = new List<Type>();

        /// <summary>Registers the scheduled task handler. Called when the endpoint starts, so tasks can be scheduled after start.</summary>
        internal void RegisterScheduledTaskHandler()
        {
            if (IsSendOnly) return;
            var name = MessageType(typeof(ScheduledTask));
            if (Handlers.ContainsKey(name)) return;
            AddRegistration(name, new Registration(typeof(ScheduledTask), false, async (message, context) =>
            {
                var scheduled = (ScheduledTask)message;
                // Tasks live in memory, as in NServiceBus. A restarted endpoint drops them until they are scheduled again.
                if (!ScheduledTasks.TryGetValue(scheduled.TaskId, out var task)) return;
                await task(context).ConfigureAwait(false);
                await context.SendLocalAt(scheduled, DateTimeOffset.UtcNow + scheduled.Every).ConfigureAwait(false);
            }));
        }

        public OutboxSettings EnableOutbox() => new OutboxSettings(this);
        public void DisableInstallers() => installersEnabled = false;
        public UnitOfWorkSettings UnitOfWork() => new UnitOfWorkSettings();
        public HostInfoSettings UniquelyIdentifyRunningInstance() => new HostInfoSettings();
        public void License(string licenseText) { }
        public void LicensePath(string licenseFilePath) { }
        public void TimeToWaitBeforeTriggeringCriticalErrorOnTimeoutOutages(TimeSpan timeToWait)
        {
            if (timeToWait <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(timeToWait));
            CriticalErrorThreshold = timeToWait;
        }

        public void ExecuteTheseHandlersFirst(params Type[] handlerTypes) => HandlerOrder.AddRange(handlerTypes ?? throw new ArgumentNullException(nameof(handlerTypes)));
        public void ExecuteTheseHandlersFirst(IEnumerable<Type> handlerTypes) => HandlerOrder.AddRange(handlerTypes ?? throw new ArgumentNullException(nameof(handlerTypes)));
    }
}
