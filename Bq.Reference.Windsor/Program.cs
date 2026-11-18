using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Bq.Messaging;
using Bq.Persistence.Sqlite;
using Castle.MicroKernel.Registration;
using Castle.MicroKernel.Lifestyle;
using Castle.Windsor;

namespace Bq.Reference.Windsor
{
    public class PlaceOrder { public string OrderId { get; set; } }
    public class PrepareOrder { public string OrderId { get; set; } }

    // Reused both by the console request and by a message handler.
    public class OrderMessages
    {
        readonly IUniformSession session;
        public OrderMessages(IUniformSession session) { this.session = session; }
        public Task Place(string id) => session.SendLocal(new PlaceOrder { OrderId = id });
        public Task Prepare(string id) => session.SendLocal(new PrepareOrder { OrderId = id });
    }

    // Commands share the current Bq transaction.
    public class OrderRepository
    {
        readonly DbConnection connection;
        readonly DbTransaction transaction;

        public OrderRepository(DbConnection connection, DbTransaction transaction)
        {
            this.connection = connection;
            this.transaction = transaction;
        }
        public async Task Save(string id, string state)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.Transaction = transaction;
                cmd.CommandText = "INSERT OR REPLACE INTO sample_orders(order_id,state) VALUES(@id,@state)";
                var idParam = cmd.CreateParameter(); idParam.ParameterName = "@id"; idParam.Value = id; cmd.Parameters.Add(idParam);
                var stateParam = cmd.CreateParameter(); stateParam.ParameterName = "@state"; stateParam.Value = state; cmd.Parameters.Add(stateParam);
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }
    public class PlaceOrderHandler : IHandleMessages<PlaceOrder>
    {
        readonly OrderRepository orders;
        readonly OrderMessages messages;
        public PlaceOrderHandler(OrderRepository orders, OrderMessages messages) { this.orders = orders; this.messages = messages; }
        public async Task Handle(PlaceOrder message, IMessageHandlerContext context)
        {
            await orders.Save(message.OrderId, "placed");
            // This local message and the database write commit together via the outbox.
            await messages.Prepare(message.OrderId);
            Console.WriteLine("Placed " + message.OrderId);
        }
    }
    public class PrepareOrderHandler : IHandleMessages<PrepareOrder>
    {
        readonly OrderRepository orders;
        public PrepareOrderHandler(OrderRepository orders) { this.orders = orders; }
        public async Task Handle(PrepareOrder message, IMessageHandlerContext context)
        {
            await orders.Save(message.OrderId, "prepared");
            Console.WriteLine("Prepared " + message.OrderId);
        }
    }

    // Explicit adapter: Bq calls the factory per attempt and Windsor owns the scoped objects.
    // The AsyncLocal context follows awaits and isolates concurrent message deliveries.
    public static class WindsorHandlerScope
    {
        static readonly AsyncLocal<IMessageHandlerContext> current = new AsyncLocal<IMessageHandlerContext>();
        public static IMessageHandlerContext Current => current.Value;
        public static IHandleMessages<T> Create<T>(IWindsorContainer container, IMessageHandlerContext context) => new ScopedHandler<T>(container, context);
        sealed class ScopedHandler<T> : IHandleMessages<T>
        {
            readonly IWindsorContainer container;
            readonly IMessageHandlerContext context;
            public ScopedHandler(IWindsorContainer container, IMessageHandlerContext context) { this.container = container; this.context = context; }
            public async Task Handle(T message, IMessageHandlerContext ignored)
            {
                var previous = current.Value;
                current.Value = context;
                try
                {
                    using (container.BeginScope())
                        await container.Resolve<IHandleMessages<T>>().Handle(message, context);
                }
                finally { current.Value = previous; }
            }
        }
    }

    static class Program
    {
        static DbConnection Connection(string path)
        {
            // Default to the Mono ADO.NET provider; another compatible SQLite ADO.NET
            // connection type can be supplied without changing Bq's persistence provider.
            var provider = Environment.GetEnvironmentVariable("BQ_SQLITE_CONNECTION_TYPE") ??
                "Mono.Data.Sqlite.SqliteConnection, Mono.Data.Sqlite, Version=4.0.0.0, Culture=neutral, PublicKeyToken=0738eb9f132ed756";
            var connection = (DbConnection)Activator.CreateInstance(Type.GetType(provider, true));
            connection.ConnectionString = "Data Source=" + path + ";Default Timeout=5";
            return connection;
        }
        static async Task Main()
        {
            const string endpointName = "bq.windsor.reference";
            var uri = new Uri(Environment.GetEnvironmentVariable("BQ_RABBITMQ_URI") ?? "amqp://guest:guest@127.0.0.1:5672/");
            var path = Environment.GetEnvironmentVariable("BQ_SAMPLE_DB") ?? "bq-windsor.db";
            var persistence = new SqlitePersistence(() => Connection(path), endpointName);
            await persistence.Initialize();
            using (var connection = Connection(path))
            {
                await connection.OpenAsync();
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = "CREATE TABLE IF NOT EXISTS sample_orders(order_id TEXT PRIMARY KEY,state TEXT NOT NULL)";
                    await cmd.ExecuteNonQueryAsync();
                }
            }
            using (var container = new WindsorContainer())
            {
                RabbitEndpoint endpoint = null;
                container.Register(
                    Component.For<IMessageHandlerContext>().UsingFactoryMethod(() => WindsorHandlerScope.Current ?? throw new InvalidOperationException("No active message scope.")).LifestyleScoped(),
                    Component.For<IUniformSession>().UsingFactoryMethod(() => WindsorHandlerScope.Current != null ? WindsorHandlerScope.Current.UniformSession : endpoint.CreateUniformSession()).LifestyleScoped(),
                    Component.For<OrderMessages>().LifestyleScoped(),
                    Component.For<OrderRepository>().UsingFactoryMethod(() =>
                    {
                        var persistenceSession = WindsorHandlerScope.Current?.PersistenceSession ??
                            throw new InvalidOperationException("No active message scope.");
                        return new OrderRepository(persistenceSession.Connection, persistenceSession.Transaction);
                    }).LifestyleScoped(),
                    Component.For<IHandleMessages<PlaceOrder>>().ImplementedBy<PlaceOrderHandler>().LifestyleScoped(),
                    Component.For<IHandleMessages<PrepareOrder>>().ImplementedBy<PrepareOrderHandler>().LifestyleScoped());
                var config = new EndpointConfiguration(endpointName, uri) { Persistence = persistence };
                config.Handle<PlaceOrder>(ctx => WindsorHandlerScope.Create<PlaceOrder>(container, ctx));
                config.Handle<PrepareOrder>(ctx => WindsorHandlerScope.Create<PrepareOrder>(container, ctx));
                endpoint = RabbitEndpoint.Start(config);
                using (endpoint)
                {
                    var id = await endpoint.ExecuteTransaction(async session =>
                    {
                        var orderId = Guid.NewGuid().ToString("N");
                        await new OrderRepository(session.Connection, session.Transaction).Save(orderId, "requested");
                        using (container.BeginScope())
                            await container.Resolve<OrderMessages>().Place(orderId);
                        return orderId;
                    });
                    var prepared = false;
                    for (var i = 0; i < 150 && !prepared; i++)
                    {
                        using (var connection = Connection(path))
                        {
                            await connection.OpenAsync();
                            using (var cmd = connection.CreateCommand())
                            {
                                cmd.CommandText = "SELECT state FROM sample_orders WHERE order_id=@id";
                                var parameter = cmd.CreateParameter(); parameter.ParameterName = "@id"; parameter.Value = id; cmd.Parameters.Add(parameter);
                                prepared = (string)await cmd.ExecuteScalarAsync() == "prepared";
                            }
                        }
                        if (!prepared) await Task.Delay(100);
                    }
                    if (!prepared) throw new TimeoutException("The order did not reach the prepared state.");
                    await endpoint.StopAsync();
                    Console.WriteLine("Reference workflow completed; data: " + path);
                }
            }
        }
    }
}
