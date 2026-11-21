using System;
using System.Data.Common;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using Bq.Messaging;
using Bq.Persistence.Oracle;
using Oracle.ManagedDataAccess.Client;

public class Work { public int Id { get; set; } public string Name { get; set; } }

public class AppContext : DbContext
{
    public AppContext(DbConnection connection) : base(connection, contextOwnsConnection: false) { Database.SetInitializer<AppContext>(null); }
    public DbSet<Work> Work { get; set; }
    protected override void OnModelCreating(DbModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(Environment.GetEnvironmentVariable("BQ_ORACLE_SCHEMA") ?? "BQ");
        modelBuilder.Entity<Work>().ToTable("APP_WORK");
        modelBuilder.Entity<Work>().Property(w => w.Id).HasColumnName("ID").HasDatabaseGeneratedOption(System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.None);
        modelBuilder.Entity<Work>().Property(w => w.Name).HasColumnName("NAME");
    }
}

static class Program
{
    static void Assert(bool value, string name) { if (!value) throw new Exception("Failed: " + name); Console.WriteLine("ok: " + name); }

    static int Count(string sql, string connectionString)
    {
        using (var connection = new OracleConnection(connectionString))
        {
            connection.Open();
            using (var command = connection.CreateCommand()) { command.CommandText = sql; return Convert.ToInt32(command.ExecuteScalar()); }
        }
    }

    static async Task Main()
    {
        var connectionString = Environment.GetEnvironmentVariable("BQ_ORACLE_CONNECTION_STRING");
        if (string.IsNullOrEmpty(connectionString)) { Console.WriteLine("Set BQ_ORACLE_CONNECTION_STRING to run this check."); return; }
        using (var setup = new OracleConnection(connectionString))
        {
            setup.Open();
            using (var command = setup.CreateCommand())
            {
                // The application table; Bq's own tables come from migrations/001_create_bq_tables.sql.
                command.CommandText = "BEGIN EXECUTE IMMEDIATE 'CREATE TABLE app_work (id NUMBER(10) PRIMARY KEY, name NVARCHAR2(200))'; " +
                    "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;";
                command.ExecuteNonQuery();
            }
        }
        var endpoint = "bq-ef6-" + Guid.NewGuid().ToString("N");
        var store = new OraclePersistence(() => new OracleConnection(connectionString), endpoint);
        var id = new Random().Next(1, 1000000);

        // The EF save and the outbox insert share Bq's connection and transaction, so they commit together.
        using (var session = await store.OpenSession())
        {
            Assert(System.Transactions.Transaction.Current == null, "no ambient System.Transactions transaction (no MSDTC)");
            using (var context = new AppContext(session.Connection))
            {
                context.Database.UseTransaction(session.Transaction);
                context.Work.Add(new Work { Id = id, Name = "committed" });
                context.SaveChanges();
            }
            await session.StoreOutgoing(new OutgoingMessage { Id = "ef-commit", Type = "t", Exchange = "", RoutingKey = endpoint, Body = new byte[] { 1 } });
            session.Commit();
        }
        Assert(Count("SELECT COUNT(*) FROM app_work WHERE id = " + id, connectionString) == 1, "EF write is visible after commit");
        Assert((await store.NextOutgoing(endpoint))?.Id == "ef-commit", "outbox row is visible after commit");

        await store.MarkDispatched(endpoint, "ef-commit");

        // Without a commit, both roll back together.
        var rolledBackId = id + 1;
        using (var session = await store.OpenSession())
        {
            using (var context = new AppContext(session.Connection))
            {
                context.Database.UseTransaction(session.Transaction);
                context.Work.Add(new Work { Id = rolledBackId, Name = "rolled back" });
                context.SaveChanges();
            }
            await session.StoreOutgoing(new OutgoingMessage { Id = "ef-rollback", Type = "t", Exchange = "", RoutingKey = endpoint, Body = new byte[] { 1 } });
        }
        Assert(Count("SELECT COUNT(*) FROM app_work WHERE id = " + rolledBackId, connectionString) == 0, "EF write rolls back with the session");
        Assert(await store.NextOutgoing(endpoint) == null, "outbox row rolls back with the session");
        Console.WriteLine("EF6 over Oracle: connection and transaction sharing verified.");
    }
}
