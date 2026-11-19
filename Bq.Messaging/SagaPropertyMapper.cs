using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;

namespace Bq.Messaging
{
    public sealed class SagaPropertyMapper<TData> where TData : class, new()
    {
        interface IMapping
        {
            void Apply<TSaga>(SagaMappings<TSaga, TData> mappings) where TSaga : Saga<TData>;
        }

        sealed class Mapping<TMessage> : IMapping where TMessage : class
        {
            readonly Func<TMessage, object> getValue;
            readonly PropertyInfo sagaProperty;

            public Mapping(Func<TMessage, object> getValue, PropertyInfo sagaProperty)
            {
                this.getValue = getValue;
                this.sagaProperty = sagaProperty;
            }

            public void Apply<TSaga>(SagaMappings<TSaga, TData> mappings) where TSaga : Saga<TData> =>
                mappings.AddMappedMessage<TMessage>(message =>
                {
                    var value = getValue(message);
                    var key = Convert.ToString(value, CultureInfo.InvariantCulture);
                    if (string.IsNullOrEmpty(key)) throw new InvalidOperationException("Saga correlation key must not be empty.");
                    return (key, data => sagaProperty.SetValue(data, value));
                });
        }

        readonly Dictionary<Type, IMapping> mappings = new Dictionary<Type, IMapping>();
        PropertyInfo sagaProperty;

        public MessageMapping<TMessage> ConfigureMapping<TMessage>(Expression<Func<TMessage, object>> property)
            where TMessage : class => new MessageMapping<TMessage>(property, Add);

        public SagaMapping MapSaga(Expression<Func<TData, object>> property) => new SagaMapping(this, property);

        internal void Add<TMessage>(Expression<Func<TData, object>> data, Expression<Func<TMessage, object>> message)
            where TMessage : class
        {
            var target = GetProperty(data);
            var source = GetProperty(message);
            var keyType = Nullable.GetUnderlyingType(target.PropertyType) ?? target.PropertyType;
            if (keyType != (Nullable.GetUnderlyingType(source.PropertyType) ?? source.PropertyType) ||
                target.SetMethod?.IsPublic != true || !IsSupportedKey(keyType))
                throw new ArgumentException("Saga correlation requires compatible scalar properties and a writable saga property.");
            if (sagaProperty != null && !sagaProperty.Equals(target))
                throw new InvalidOperationException("All saga messages must map to the same saga property.");
            if (mappings.ContainsKey(typeof(TMessage)))
                throw new InvalidOperationException("A saga correlation is already registered for " + typeof(TMessage).FullName);
            mappings.Add(typeof(TMessage), new Mapping<TMessage>(message.Compile(), target));
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

        public sealed class SagaMapping
        {
            readonly SagaPropertyMapper<TData> owner;
            readonly Expression<Func<TData, object>> data;

            internal SagaMapping(SagaPropertyMapper<TData> owner, Expression<Func<TData, object>> data)
            {
                this.owner = owner;
                this.data = data;
            }

            public SagaMapping ToMessage<TMessage>(Expression<Func<TMessage, object>> message) where TMessage : class
            {
                owner.Add(data, message);
                return this;
            }
        }

        public sealed class MessageMapping<TMessage> where TMessage : class
        {
            readonly Expression<Func<TMessage, object>> message;
            readonly Action<Expression<Func<TData, object>>, Expression<Func<TMessage, object>>> add;

            internal MessageMapping(Expression<Func<TMessage, object>> message,
                Action<Expression<Func<TData, object>>, Expression<Func<TMessage, object>>> add)
            {
                this.message = message;
                this.add = add;
            }

            public void ToSaga(Expression<Func<TData, object>> data) => add(data, message);
        }
    }
}
