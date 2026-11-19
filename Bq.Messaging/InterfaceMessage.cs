using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;

namespace Bq.Messaging
{
    internal static class InterfaceMessage
    {
        static readonly ConcurrentDictionary<Type, Lazy<Type>> implementations = new ConcurrentDictionary<Type, Lazy<Type>>();
        static readonly ConcurrentDictionary<Type, Type> contracts = new ConcurrentDictionary<Type, Type>();
        static readonly Lazy<ModuleBuilder> module = new Lazy<ModuleBuilder>(() =>
            AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("Bq.InterfaceMessages"), AssemblyBuilderAccess.Run)
                .DefineDynamicModule("Messages"));

        public static Type ContractFor(Type implementation) =>
            contracts.TryGetValue(implementation, out var contract) ? contract : implementation;

        public static Type SerializationType(Type contract) => contract.IsInterface
            ? implementations.GetOrAdd(contract, type => new Lazy<Type>(() => Define(type))).Value
            : contract;

        public static object Create(Type contract)
        {
            var type = SerializationType(contract);
            if (!type.IsClass || type.IsAbstract || type.GetConstructor(Type.EmptyTypes) == null)
                throw new ArgumentException("Message initializers require an interface or a class with a public parameterless constructor.", nameof(contract));
            return Activator.CreateInstance(type);
        }

        public static object SerializationValue(Type contract, Type implementation, object message)
        {
            if (!contract.IsInterface || implementation.IsInstanceOfType(message)) return message;
            var copy = Activator.CreateInstance(implementation);
            foreach (var property in Properties(contract))
                implementation.GetProperty(property.Name).SetValue(copy, property.GetValue(message));
            return copy;
        }

        static PropertyInfo[] Properties(Type contract) =>
            contract.GetInterfaces().Concat(new[] { contract })
                .SelectMany(type => type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
                .ToArray();

        static Type Define(Type contract)
        {
            var interfaces = contract.GetInterfaces().Concat(new[] { contract }).ToArray();
            var groups = Properties(contract).GroupBy(property => property.Name).ToArray();
            var accessors = groups.SelectMany(group => group).SelectMany(property => property.GetAccessors()).ToArray();
            if (!contract.IsVisible || contract.ContainsGenericParameters ||
                interfaces.SelectMany(type => type.GetMethods()).Any(method => !accessors.Contains(method)))
                throw new NotSupportedException("Interface messages must be public interfaces with data properties only.");

            foreach (var group in groups)
            {
                var type = group.First().PropertyType;
                if (group.Any(property => property.PropertyType != type || !property.CanRead || !property.CanWrite ||
                    property.GetIndexParameters().Length != 0) || !type.IsVisible || type.IsByRef || type.IsPointer)
                    throw new NotSupportedException("Interface message properties must have a public getter and setter and an unambiguous type.");
            }

            var target = module.Value;
            lock (target)
            {
                var builder = target.DefineType("Message_" + Guid.NewGuid().ToString("N"),
                    TypeAttributes.Public | TypeAttributes.Sealed | TypeAttributes.Class);
                builder.AddInterfaceImplementation(contract);
                builder.DefineDefaultConstructor(MethodAttributes.Public);
                foreach (var group in groups)
                    AddProperty(builder, group);
                var implementation = builder.CreateTypeInfo().AsType();
                contracts.TryAdd(implementation, contract);
                return implementation;
            }
        }

        static void AddProperty(TypeBuilder builder, IGrouping<string, PropertyInfo> group)
        {
            const MethodAttributes accessor = MethodAttributes.Public | MethodAttributes.SpecialName |
                MethodAttributes.HideBySig | MethodAttributes.Virtual | MethodAttributes.Final | MethodAttributes.NewSlot;
            var type = group.First().PropertyType;
            var field = builder.DefineField("_" + group.Key, type, FieldAttributes.Private);
            var property = builder.DefineProperty(group.Key, PropertyAttributes.None, type, Type.EmptyTypes);
            var getter = builder.DefineMethod("get_" + group.Key, accessor, type, Type.EmptyTypes);
            var setter = builder.DefineMethod("set_" + group.Key, accessor, typeof(void), new[] { type });

            var get = getter.GetILGenerator();
            get.Emit(OpCodes.Ldarg_0);
            get.Emit(OpCodes.Ldfld, field);
            get.Emit(OpCodes.Ret);

            var set = setter.GetILGenerator();
            set.Emit(OpCodes.Ldarg_0);
            set.Emit(OpCodes.Ldarg_1);
            set.Emit(OpCodes.Stfld, field);
            set.Emit(OpCodes.Ret);

            property.SetGetMethod(getter);
            property.SetSetMethod(setter);
            foreach (var declaration in group)
            {
                builder.DefineMethodOverride(getter, declaration.GetMethod);
                builder.DefineMethodOverride(setter, declaration.SetMethod);
            }
        }
    }
}
