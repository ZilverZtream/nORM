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

        /// <summary>
        /// Returns no accessor methods because a shadow property has no backing members.
        /// </summary>
        /// <param name="nonPublic">Ignored.</param>
        /// <returns>An empty array.</returns>
        public override MethodInfo[] GetAccessors(bool nonPublic) => Array.Empty<MethodInfo>();

        /// <summary>
        /// Gets the <c>get</c> accessor method. Always returns <c>null</c> for shadow properties
        /// because they do not expose runtime accessors.
        /// </summary>
        /// <param name="nonPublic">Ignored.</param>
        /// <returns><c>null</c> in all cases.</returns>
        public override MethodInfo? GetGetMethod(bool nonPublic) => null;

        /// <summary>
        /// Retrieves index parameters for the property. Shadow properties are not indexers so an empty
        /// array is returned.
        /// </summary>
        /// <returns>An empty <see cref="ParameterInfo"/> array.</returns>
        public override ParameterInfo[] GetIndexParameters() => Array.Empty<ParameterInfo>();

        /// <summary>
        /// Gets the <c>set</c> accessor method. Shadow properties do not have accessors and therefore
        /// this method always returns <c>null</c>.
        /// </summary>
        /// <param name="nonPublic">Ignored.</param>
        /// <returns><c>null</c>.</returns>
        public override MethodInfo? GetSetMethod(bool nonPublic) => null;

        /// <summary>
        /// Attempting to read the value of a shadow property is not supported and will always throw
        /// <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="obj">The object instance. Ignored.</param>
        /// <param name="index">Index values. Ignored.</param>
        /// <returns>Nothing; this method always throws.</returns>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public override object? GetValue(object? obj, object?[]? index) => throw new NotSupportedException();

        /// <summary>
        /// Attempting to assign a value to a shadow property is not supported and will always throw
        /// <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="obj">The object instance. Ignored.</param>
        /// <param name="value">The value to set. Ignored.</param>
        /// <param name="index">Index values. Ignored.</param>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public override void SetValue(object? obj, object? value, object?[]? index) => throw new NotSupportedException();

        /// <summary>
        /// Overload of <see cref="GetValue(object,object?[])"/> that also accepts binding information.
        /// Always throws <see cref="NotSupportedException"/> for shadow properties.
        /// </summary>
        public override object? GetValue(object? obj, BindingFlags invokeAttr, Binder? binder, object?[]? index, CultureInfo? culture)
            => throw new NotSupportedException();

        /// <summary>
        /// Overload of <see cref="SetValue(object,object,object?[])"/> that also accepts binding information.
        /// Always throws <see cref="NotSupportedException"/> for shadow properties.
        /// </summary>
        public override void SetValue(object? obj, object? value, BindingFlags invokeAttr, Binder? binder, object?[]? index, CultureInfo? culture)
            => throw new NotSupportedException();

        /// <summary>
        /// Shadow properties do not carry custom attributes; this method therefore returns an empty array.
        /// </summary>
        /// <param name="inherit">Ignored.</param>
        /// <returns>An empty attribute array.</returns>
        public override object[] GetCustomAttributes(bool inherit) => Array.Empty<object>();

        /// <summary>
        /// Shadow properties do not carry custom attributes; this method therefore returns an empty array.
        /// </summary>
        /// <param name="attributeType">The type of attribute to search for. Ignored.</param>
        /// <param name="inherit">Ignored.</param>
        /// <returns>An empty attribute array.</returns>
        public override object[] GetCustomAttributes(Type attributeType, bool inherit) => Array.Empty<object>();

        /// <summary>
        /// Indicates whether a custom attribute is defined. Always returns <c>false</c> for shadow properties
        /// because they cannot have attributes.
        /// </summary>
        /// <param name="attributeType">The attribute type to search for.</param>
        /// <param name="inherit">Ignored.</param>
        /// <returns><c>false</c> in all cases.</returns>
        public override bool IsDefined(Type attributeType, bool inherit) => false;
    }
}
