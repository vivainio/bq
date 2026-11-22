using System;
using System.Data.Common;

namespace Bq.Messaging.Tests
{
    // The ADO.NET SQLite driver for the runtime: Mono's on .NET Framework, Microsoft.Data.Sqlite on .NET.
    static class SqliteDriver
    {
#if NETFRAMEWORK
        const string ConnectionType = "Mono.Data.Sqlite.SqliteConnection, Mono.Data.Sqlite, Version=4.0.0.0, Culture=neutral, PublicKeyToken=0738eb9f132ed756";
#else
        const string ConnectionType = "Microsoft.Data.Sqlite.SqliteConnection, Microsoft.Data.Sqlite";
#endif

        public static DbConnection Connect(string path)
        {
            var connection = (DbConnection)Activator.CreateInstance(Type.GetType(ConnectionType, true));
            connection.ConnectionString = "Data Source=" + path + ";Default Timeout=5";
            return connection;
        }
    }
}
