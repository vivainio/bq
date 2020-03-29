using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using FastMember;

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
        
    }
    public class FastMemberOrm<T> where T: new()
    {
        private readonly TypeAccessor _accessor;
        private readonly Dictionary<string, string> _lowercaseMap;
        private Action<string, object, T> _unknownMapper;

        public FastMemberOrm()
        {
            this._accessor = TypeAccessor.Create(typeof(T));
            this._lowercaseMap = this._accessor.GetMembers().ToDictionary(i => i.Name.ToLowerInvariant(), i => i.Name);
        }

        public FastMemberOrm<T> OmitProperties(params string[] properties)
        {
            foreach (var prop in properties)
            {
                _lowercaseMap.Remove(prop.ToLowerInvariant());
            }

            return this;

        }

        public FastMemberOrm<T> UseFallback(Action<string, object, T> specialMapper)
        {
            _unknownMapper = specialMapper;
            return this;
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

                var trivialMapperFound = _lowercaseMap.TryGetValue(name, out var propName);
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
                    _accessor[newObject, propName] = value;
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