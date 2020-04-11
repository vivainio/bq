using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using FastMember;
using Google.Protobuf.WellKnownTypes;
using Enum = System.Enum;
using Type = System.Type;

namespace Bq
{
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
                    return Timestamp.FromDateTime(DateTime.SpecifyKind(dateTime, DateTimeKind.Utc));
                case Decimal dec when targetType.IsEnum:
                    return (object) (int) dec;
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
                    var reboxed = ReboxToType(value, propData.Type);
                    try
                    {
                        _accessor[newObject, propData.Name] = reboxed;
                    }
                    catch (InvalidCastException)
                    {
                        throw;
                    }
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