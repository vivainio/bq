using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;

namespace Bq.Messaging
{
    public interface IToSagaExpression<TData> where TData : class
    {
        void ToSaga(Expression<Func<TData, object>> sagaEntityProperty);
    }

    public sealed class SagaPropertyMapper<TData> where TData : class, new()
    {
        interface IMapping
        {
            void Apply<TSaga>(SagaMappings<TSaga, TData> mappings) where TSaga : Saga<TData>;
        }

        sealed class Mapping<TMessage> : IMapping where TMessage : class
        {
            readonly Func<TMessage, IMessageHandlerContext, object> getValue;
            readonly PropertyInfo sagaProperty;

            public Mapping(Func<TMessage, IMessageHandlerContext, object> getValue, PropertyInfo sagaProperty)
            {
                this.getValue = getValue;
                this.sagaProperty = sagaProperty;
            }

            public void Apply<TSaga>(SagaMappings<TSaga, TData> mappings) where TSaga : Saga<TData> =>
                mappings.AddMappedMessage<TMessage>((message, context) =>
                {
                    var value = getValue(message, context);
                    var key = Convert.ToString(value, CultureInfo.InvariantCulture);
                    if (string.IsNullOrEmpty(key)) throw new InvalidOperationException("Saga correlation key must not be empty.");
                    return (key, data => sagaProperty.SetValue(data, value));
                });
        }

        readonly Dictionary<Type, IMapping> mappings = new Dictionary<Type, IMapping>();
        PropertyInfo sagaProperty;
        internal PropertyInfo CorrelationProperty => sagaProperty;
        internal IEnumerable<Type> MappedMessageTypes => mappings.Keys;

        public ToSagaExpression<TData, TMessage> ConfigureMapping<TMessage>(Expression<Func<TMessage, object>> property)
            where TMessage : class => new ToSagaExpression<TData, TMessage>(property, Add);

        public IToSagaExpression<TData> ConfigureHeaderMapping<TMessage>(string headerName)
            where TMessage : class => new HeaderMapping<TMessage>(this, headerName);

        public CorrelatedSagaPropertyMapper<TData> MapSaga(Expression<Func<TData, object>> property) => new CorrelatedSagaPropertyMapper<TData>(this, property);

        internal void Add<TMessage>(Expression<Func<TData, object>> data, Expression<Func<TMessage, object>> message)
            where TMessage : class
        {
            var target = GetProperty(data);
            var source = GetProperty(message);
            var keyType = Nullable.GetUnderlyingType(target.PropertyType) ?? target.PropertyType;
            if (keyType != (Nullable.GetUnderlyingType(source.PropertyType) ?? source.PropertyType))
                throw new ArgumentException("Saga correlation requires compatible scalar properties.");
            var read = message.Compile();
            Register<TMessage>(target, (value, _) => read(value));
        }

        void AddHeader<TMessage>(Expression<Func<TData, object>> data, string headerName) where TMessage : class
        {
            var target = GetProperty(data);
            var type = Nullable.GetUnderlyingType(target.PropertyType) ?? target.PropertyType;
            Register<TMessage>(target, (_, context) =>
            {
                var value = context.GetHeader(headerName);
                if (type == typeof(string)) return value;
                if (type == typeof(Guid)) return Guid.Parse(value);
                if (type.IsEnum) return Enum.Parse(type, value);
                return Convert.ChangeType(value, type, CultureInfo.InvariantCulture);
            });
        }

        void Register<TMessage>(PropertyInfo target, Func<TMessage, IMessageHandlerContext, object> read) where TMessage : class
        {
            var type = Nullable.GetUnderlyingType(target.PropertyType) ?? target.PropertyType;
            if (target.SetMethod?.IsPublic != true || !IsSupportedKey(type))
                throw new ArgumentException("Saga correlation requires a writable scalar saga property.");
            if (sagaProperty != null && !sagaProperty.Equals(target))
                throw new InvalidOperationException("All saga messages must map to the same saga property.");
            if (mappings.ContainsKey(typeof(TMessage)))
                throw new InvalidOperationException("A saga correlation is already registered for " + typeof(TMessage).FullName);
            mappings.Add(typeof(TMessage), new Mapping<TMessage>(read, target));
            sagaProperty = target;
        }

        static PropertyInfo GetProperty(LambdaExpression selector)
        {
            if (selector == null) throw new ArgumentNullException(nameof(selector));
            var member = (selector.Body is UnaryExpression conversion && conversion.NodeType == ExpressionType.Convert
                ? conversion.Operand : selector.Body) as MemberExpression;
            if (member?.Member is PropertyInfo property && member.Expression == selector.Parameters[0])
                return property;
            throw new ArgumentException("Select a direct property.", nameof(selector));
        }

        static bool IsSupportedKey(Type type) => type == typeof(string) || type == typeof(Guid) || type.IsEnum ||
            type == typeof(byte) || type == typeof(sbyte) || type == typeof(short) || type == typeof(ushort) ||
            type == typeof(int) || type == typeof(uint) || type == typeof(long) || type == typeof(ulong);

        internal void Apply<TSaga>(SagaMappings<TSaga, TData> target) where TSaga : Saga<TData>
        {
            foreach (var mapping in mappings.Values) mapping.Apply(target);
        }

        sealed class HeaderMapping<TMessage> : IToSagaExpression<TData> where TMessage : class
        {
            readonly SagaPropertyMapper<TData> owner;
            readonly string headerName;

            internal HeaderMapping(SagaPropertyMapper<TData> owner, string headerName)
            {
                this.owner = owner;
                this.headerName = string.IsNullOrWhiteSpace(headerName)
                    ? throw new ArgumentException("Header name is required.", nameof(headerName)) : headerName;
            }

            public void ToSaga(Expression<Func<TData, object>> data) => owner.AddHeader<TMessage>(data, headerName);
        }
    }

    public sealed class CorrelatedSagaPropertyMapper<TData> where TData : class, new()
    {
        readonly SagaPropertyMapper<TData> owner;
        readonly Expression<Func<TData, object>> data;

        internal CorrelatedSagaPropertyMapper(SagaPropertyMapper<TData> owner, Expression<Func<TData, object>> data)
        {
            this.owner = owner;
            this.data = data;
        }

        public CorrelatedSagaPropertyMapper<TData> ToMessageHeader<TMessage>(string headerName) where TMessage : class
        {
            owner.ConfigureHeaderMapping<TMessage>(headerName).ToSaga(data);
            return this;
        }

        public CorrelatedSagaPropertyMapper<TData> ToMessage<TMessage>(Expression<Func<TMessage, object>> message) where TMessage : class
        {
            owner.Add(data, message);
            return this;
        }
    }

    public sealed class ToSagaExpression<TData, TMessage> : IToSagaExpression<TData> where TData : class where TMessage : class
    {
        readonly Expression<Func<TMessage, object>> message;
        readonly Action<Expression<Func<TData, object>>, Expression<Func<TMessage, object>>> add;

        internal ToSagaExpression(Expression<Func<TMessage, object>> message,
            Action<Expression<Func<TData, object>>, Expression<Func<TMessage, object>>> add)
        {
            this.message = message;
            this.add = add;
        }

        public void ToSaga(Expression<Func<TData, object>> data) => add(data, message);
    }
}
