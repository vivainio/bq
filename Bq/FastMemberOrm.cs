using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using FastMember;
using Google.Protobuf.WellKnownTypes;
using Enum = System.Enum;
using Type = System.Type;

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
    public class FastMemberOrm<T> where T : new()
    {
        private readonly TypeAccessor _accessor;
        private readonly Dictionary<string, (string Name, Type Type)> _lowercaseMap;
        private Action<string, object, T> _unknownMapper;

        public FastMemberOrm()
        {
            this._accessor = TypeAccessor.Create(typeof(T));
            this._lowercaseMap = this._accessor.GetMembers().ToDictionary(i => i.Name.ToLowerInvariant(), i => 
                (i.Name, i.Type));
            
        }

        public (string Name, Type Type)[] Props => _lowercaseMap.Values.ToArray();

        public FastMemberOrm<T> OmitProperties(params string[] properties)
        {
            foreach (var prop in properties)
            {
                _lowercaseMap.Remove(prop.ToLowerInvariant());
            }

            return this;
        }

        public FastMemberOrm<T> UseFallbackWhenReading(Action<string, object, T> specialMapper)
        {
            _unknownMapper = specialMapper;
            return this;
        }

        private static object ReboxToType(object src, Type targetType)
        {
            switch (src)
            {
                case DateTime dateTime when targetType == typeof(Timestamp):
                    return Timestamp.FromDateTime(dateTime);
                default:
                    return src;
                     
            }
        }
        private static object ReboxToDb(object value)
        {
            if (value is Enum @enum)
            {
                return Convert.ToInt16(@enum);
            }

            if (value is Timestamp ts)
            {
                return (object) ts.ToDateTime();
            }

            return value;
        }
        
        

        
        public void AddParamsToCommand(DbCommand command, T entity)
        {
            foreach (var prop in _lowercaseMap)
            {
                var param = command.CreateParameter();
                param.ParameterName = prop.Key.ToUpperInvariant();
                var value = _accessor[entity, prop.Value.Name];
                param.Value = ReboxToDb(value);
                command.Parameters.Add(param);
            }
        }

        public void CopyReaderRowToObject(DbDataReader dataReader, T newObject)
        {
            for (int i = 0; i < dataReader.FieldCount; i++)
            {
                var name = dataReader.GetName(i).ToLowerInvariant();
                if (String.IsNullOrEmpty(name))
                {
                    continue;
                }

                var trivialMapperFound = _lowercaseMap.TryGetValue(name, out var propData);
                if (!trivialMapperFound)
                {
                    if (_unknownMapper == null)
                    {
                        continue;
                    }
                }

                var value = dataReader.IsDBNull(i) ? null : dataReader.GetValue(i);
                    
                if (trivialMapperFound)
                {
                    
                    _accessor[newObject, propData.Name] = value;
                }
                else
                {
                    // "unknownmapper" map the property
                    _unknownMapper(name, value, newObject);
                }
            }
        }

    }
}