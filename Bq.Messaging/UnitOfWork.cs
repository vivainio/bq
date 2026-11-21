using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Bq.Messaging.UnitOfWork
{
    public interface IManageUnitsOfWork
    {
        Task Begin();
        Task End(Exception ex = null);
    }

    public static class UnitOfWorkConfigurationExtensions
    {
        /// <summary>
        /// Registers a unit of work created for every message attempt. NServiceBus resolves these from the container;
        /// Bq has no container, so the host supplies a factory. Units begin in registration order and end in reverse order.
        /// </summary>
        public static void RegisterUnitOfWork(this EndpointConfiguration configuration, Func<IManageUnitsOfWork> factory)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (factory == null) throw new ArgumentNullException(nameof(factory));
            var factories = configuration.UnitOfWorkFactories;
            factories.Add(factory);
            if (factories.Count == 1)
                configuration.Incoming.UsePhysicalOutermost((context, next) => Invoke(factories.ToArray(), next));
        }

        static async Task Invoke(Func<IManageUnitsOfWork>[] factories, Func<Task> next)
        {
            var units = new Stack<IManageUnitsOfWork>();
            try
            {
                foreach (var factory in factories)
                {
                    var unit = factory() ?? throw new InvalidOperationException("A unit of work factory returned null.");
                    units.Push(unit);
                    await (unit.Begin() ?? throw new InvalidOperationException("IManageUnitsOfWork.Begin returned null.")).ConfigureAwait(false);
                }
                await next().ConfigureAwait(false);
                while (units.Count > 0)
                    await (units.Pop().End() ?? throw new InvalidOperationException("IManageUnitsOfWork.End returned null.")).ConfigureAwait(false);
            }
            catch (MessageDeserializationException)
            {
                throw;
            }
            catch (Exception failure)
            {
                var trailing = new List<Exception>();
                while (units.Count > 0)
                {
                    try { await units.Pop().End(failure).ConfigureAwait(false); }
                    catch (Exception endFailure) { trailing.Add(endFailure); }
                }
                if (trailing.Count == 0) throw;
                trailing.Insert(0, failure);
                throw new AggregateException(trailing);
            }
        }
    }
}

namespace Bq.Messaging
{
    public class MessageDeserializationException : Exception
    {
        public MessageDeserializationException(string message) : base(message) { }
        public MessageDeserializationException(string message, Exception innerException) : base(message, innerException) { }
    }
}
