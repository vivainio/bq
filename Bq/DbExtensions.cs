using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

namespace Bq
{
    public static class DbExtensions
    {

        private static void Trace(string s)
        {
            Console.WriteLine(s);
        }
        public static DbDataReader ExecuteReader(this DbConnection db, string sql)
        {
            var command = db.SqlCommand(sql);
            return command.ExecuteReader();
        }

        public static void ExecuteSql(this DbConnection db, string sql)
        {
            var command = db.SqlCommand(sql);
            command.CommandText = sql;
            command.ExecuteNonQuery();
            Trace(sql);
        }

        public static void ExecuteSqlIgnoreError(this DbConnection db, string sql)
        {
            try
            {
                db.ExecuteSql(sql);
            }
            catch (DbException ex)
            {
                Console.WriteLine("Ignoring error:");
                Console.WriteLine(ex);
            }
            
        }

        public static DbCommand SqlCommand(this DbConnection db, string sql)
        {
            var command = db.CreateCommand();
            command.CommandText = sql;
            Trace(sql);
            return command;

        }

        public static void AddParameter(this DbCommand command, string name, DbType type, object value)
        {
            var param = command.CreateParameter();
            param.ParameterName = name;
            param.DbType = type;
            param.Value = value;
            command.Parameters.Add(param);
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