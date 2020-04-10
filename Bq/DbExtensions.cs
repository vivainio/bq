using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Bq
{
    public static class DbExtensions
    {
        
        public static DbDataReader ExecuteReader(this DbConnection db, string sql)
        {
            var command = db.CreateCommand();
            command.CommandText = sql;
            return command.ExecuteReader();
        }

        public static void ExecuteSql(this DbConnection db, string sql)
        {
            var command = db.CreateCommand();
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }
        public static string CreationSql<T>()
        {
            var members = typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public);
            var sb = new StringBuilder();

            string declaredCol(PropertyInfo mem)
            {
                string type = mem.PropertyType.Name switch
                {
                    "String" => "VARCHAR2(256)",
                    "ByteString" => "BLOB",
                    "Timestamp" => "TIMESTAMP",
                    _ when mem.PropertyType.IsEnum => "SMALLINT",
                    _ => mem.PropertyType.Name
                };
                return $"{mem.Name.ToUpperInvariant()} {type}";

            }
            sb.Append("(");
            sb.Append(string.Join(",\n", members.Select(declaredCol)));
            sb.Append(")");
            return sb.ToString();
        }

        public static string InsertionSql(string tableName, IReadOnlyList<string> props)
        {
            var sb = new StringBuilder();
            sb.Append($"insert into {tableName} (");
            sb.Append(string.Join(", ", props));
            sb.Append(") values (");
            sb.Append(string.Join(", ", props.Select(p => ":" + p)));
            sb.Append(")");
            return sb.ToString();
        }

    }
}