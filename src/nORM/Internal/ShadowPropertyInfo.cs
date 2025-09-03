using System;
using System.Reflection;
using System.Globalization;

#nullable enable

namespace nORM.Internal
{
    internal class ShadowPropertyInfo : PropertyInfo
    {
        private readonly string _name;
        private readonly Type _propertyType;
        private readonly Type _declaringType;

        public ShadowPropertyInfo(string name, Type propertyType, Type declaringType)
        {
            _name = name;
            _propertyType = propertyType;
            _declaringType = declaringType;
        }

        public override string Name => _name;
        public override Type PropertyType => _propertyType;
        public override Type? DeclaringType => _declaringType;
        public override Type ReflectedType => _declaringType;
        public override PropertyAttributes Attributes => PropertyAttributes.None;
        public override bool CanRead => true;
        public override bool CanWrite => true;

        public override MethodInfo[] GetAccessors(bool nonPublic) => Array.Empty<MethodInfo>();
        public override MethodInfo? GetGetMethod(bool nonPublic) => null;
        public override ParameterInfo[] GetIndexParameters() => Array.Empty<ParameterInfo>();
        public override MethodInfo? GetSetMethod(bool nonPublic) => null;
        public override object? GetValue(object? obj, object?[]? index) => throw new NotSupportedException();
        public override void SetValue(object? obj, object? value, object?[]? index) => throw new NotSupportedException();
        public override object? GetValue(object? obj, BindingFlags invokeAttr, Binder? binder, object?[]? index, CultureInfo? culture) => throw new NotSupportedException();
        public override void SetValue(object? obj, object? value, BindingFlags invokeAttr, Binder? binder, object?[]? index, CultureInfo? culture) => throw new NotSupportedException();
        public override object[] GetCustomAttributes(bool inherit) => Array.Empty<object>();
        public override object[] GetCustomAttributes(Type attributeType, bool inherit) => Array.Empty<object>();
        public override bool IsDefined(Type attributeType, bool inherit) => false;
    }
}
