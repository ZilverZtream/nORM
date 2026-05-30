using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using nORM.Providers;

#nullable enable

namespace nORM.Mapping
{
    /// <summary>
    /// Describes the join (bridge) table that links two entity types in a many-to-many relationship.
    /// </summary>
    public sealed class JoinTableMapping
    {
        /// <summary>Plain join table name.</summary>
        public string TableName { get; }

        /// <summary>Optional schema containing the join table.</summary>
        public string? SchemaName { get; }

        /// <summary>Escaped join table name for use in SQL statements.</summary>
        public string EscTableName { get; }

        /// <summary>Escaped column in the join table that holds the FK to the left (owner) entity's PK.</summary>
        public string EscLeftFkColumn { get; }

        /// <summary>Escaped column in the join table that holds the FK to the right (related) entity's PK.</summary>
        public string EscRightFkColumn { get; }

        /// <summary>Escaped columns in the join table that hold the FK to the left (owner) entity's key.</summary>
        public IReadOnlyList<string> EscLeftFkColumns { get; }

        /// <summary>Escaped columns in the join table that hold the FK to the right (related) entity's key.</summary>
        public IReadOnlyList<string> EscRightFkColumns { get; }

        /// <summary>Plain left FK column name.</summary>
        public string LeftFkColumn { get; }

        /// <summary>Plain right FK column name.</summary>
        public string RightFkColumn { get; }

        /// <summary>Plain left FK column names.</summary>
        public IReadOnlyList<string> LeftFkColumns { get; }

        /// <summary>Plain right FK column names.</summary>
        public IReadOnlyList<string> RightFkColumns { get; }

        /// <summary>CLR type of the left (owner) entity.</summary>
        public Type LeftType { get; }

        /// <summary>CLR type of the right (related) entity.</summary>
        public Type RightType { get; }

        /// <summary>Name of the navigation property on the left entity.</summary>
        public string LeftNavPropertyName { get; }

        /// <summary>Name of the navigation property on the right entity (inverse side), if any.</summary>
        public string? RightNavPropertyName { get; }

        /// <summary>Gets the FK value (left entity PK) from a left entity instance.</summary>
        public Func<object, object?> LeftPkGetter { get; }

        /// <summary>Gets the FK value (right entity PK) from a right entity instance.</summary>
        public Func<object, object?> RightPkGetter { get; }

        /// <summary>Gets the ordered left key values from a left entity instance.</summary>
        public IReadOnlyList<Func<object, object?>> LeftPkGetters { get; }

        /// <summary>Gets the ordered right key values from a right entity instance.</summary>
        public IReadOnlyList<Func<object, object?>> RightPkGetters { get; }

        /// <summary>Gets the collection of related (right) entities from a left entity instance.</summary>
        public Func<object, IList?> LeftCollectionGetter { get; }

        /// <summary>Sets the collection of related (right) entities on a left entity instance.</summary>
        public Action<object, IList?> LeftCollectionSetter { get; }

        /// <summary>Gets the collection of related (left) entities from a right entity instance (inverse), if configured.</summary>
        public Func<object, IList?>? RightCollectionGetter { get; }

        /// <summary>Sets the collection of related (left) entities on a right entity instance (inverse), if configured.</summary>
        public Action<object, IList?>? RightCollectionSetter { get; }

        internal JoinTableMapping(
            string tableName,
            string? schemaName,
            string leftFkColumn,
            string rightFkColumn,
            Type leftType,
            Type rightType,
            string leftNavPropertyName,
            string? rightNavPropertyName,
            Column leftPkColumn,
            Column rightPkColumn,
            PropertyInfo leftNavProp,
            PropertyInfo? rightNavProp,
            DatabaseProvider provider)
            : this(
                tableName,
                schemaName,
                new[] { leftFkColumn },
                new[] { rightFkColumn },
                leftType,
                rightType,
                leftNavPropertyName,
                rightNavPropertyName,
                new[] { leftPkColumn },
                new[] { rightPkColumn },
                leftNavProp,
                rightNavProp,
                provider)
        {
        }

        internal JoinTableMapping(
            string tableName,
            string? schemaName,
            IReadOnlyList<string> leftFkColumns,
            IReadOnlyList<string> rightFkColumns,
            Type leftType,
            Type rightType,
            string leftNavPropertyName,
            string? rightNavPropertyName,
            IReadOnlyList<Column> leftPkColumns,
            IReadOnlyList<Column> rightPkColumns,
            PropertyInfo leftNavProp,
            PropertyInfo? rightNavProp,
            DatabaseProvider provider)
        {
            TableName = tableName;
            SchemaName = schemaName;
            EscTableName = IdentifierEscaping.EscapeTable(provider, tableName, schemaName);
            LeftFkColumns = leftFkColumns.ToArray();
            RightFkColumns = rightFkColumns.ToArray();
            LeftFkColumn = LeftFkColumns[0];
            RightFkColumn = RightFkColumns[0];
            EscLeftFkColumns = LeftFkColumns.Select(column => IdentifierEscaping.EscapeSingle(provider, column)).ToArray();
            EscRightFkColumns = RightFkColumns.Select(column => IdentifierEscaping.EscapeSingle(provider, column)).ToArray();
            EscLeftFkColumn = EscLeftFkColumns[0];
            EscRightFkColumn = EscRightFkColumns[0];
            LeftType = leftType;
            RightType = rightType;
            LeftNavPropertyName = leftNavPropertyName;
            RightNavPropertyName = rightNavPropertyName;

            LeftPkGetters = leftPkColumns.Select(column => column.Getter).ToArray();
            RightPkGetters = rightPkColumns.Select(column => column.Getter).ToArray();
            LeftPkGetter = LeftPkGetters[0];
            RightPkGetter = RightPkGetters[0];

            // Build left collection getter/setter (the nav property on leftType)
            var leftParam = Expression.Parameter(typeof(object), "e");
            var leftCast = Expression.Convert(leftParam, leftType);
            var leftGetProp = Expression.Property(leftCast, leftNavProp);
            LeftCollectionGetter = Expression.Lambda<Func<object, IList?>>(
                Expression.Convert(leftGetProp, typeof(IList)), leftParam).Compile();

            var leftValueParam = Expression.Parameter(typeof(IList), "v");
            var leftCastV = Expression.Convert(leftValueParam, leftNavProp.PropertyType);
            var leftSetProp = Expression.Call(leftCast, leftNavProp.GetSetMethod()!, leftCastV);
            LeftCollectionSetter = Expression.Lambda<Action<object, IList?>>(
                leftSetProp, leftParam, leftValueParam).Compile();

            // Build right collection getter/setter (inverse nav property on rightType), if configured
            if (rightNavProp != null)
            {
                var rightParam = Expression.Parameter(typeof(object), "e");
                var rightCast = Expression.Convert(rightParam, rightType);
                var rightGetProp = Expression.Property(rightCast, rightNavProp);
                RightCollectionGetter = Expression.Lambda<Func<object, IList?>>(
                    Expression.Convert(rightGetProp, typeof(IList)), rightParam).Compile();

                var rightValueParam = Expression.Parameter(typeof(IList), "v");
                var rightCastV = (Expression)Expression.Convert(rightValueParam, rightNavProp.PropertyType);
                var rightSetProp = Expression.Call(rightCast, rightNavProp.GetSetMethod()!, rightCastV);
                RightCollectionSetter = Expression.Lambda<Action<object, IList?>>(
                    rightSetProp, rightParam, rightValueParam).Compile();
            }
        }

        internal object? GetLeftKey(object entity) => CreateKey(LeftPkGetters, entity);

        internal object? GetRightKey(object entity) => CreateKey(RightPkGetters, entity);

        internal object?[]? GetLeftKeyValues(object entity) => GetKeyValues(LeftPkGetters, entity);

        internal object?[]? GetRightKeyValues(object entity) => GetKeyValues(RightPkGetters, entity);

        internal object? CreateLeftKeyFromValues(IReadOnlyList<object?> values, int offset = 0)
            => CreateKey(values, offset, LeftPkGetters.Count);

        internal object? CreateRightKeyFromValues(IReadOnlyList<object?> values, int offset = 0)
            => CreateKey(values, offset, RightPkGetters.Count);

        internal object?[]? GetRightKeyValuesFromKey(object key)
            => GetKeyValuesFromKey(key, RightPkGetters.Count);

        private static object? CreateKey(IReadOnlyList<Func<object, object?>> getters, object entity)
        {
            var values = new object?[getters.Count];
            for (var i = 0; i < getters.Count; i++)
            {
                values[i] = getters[i](entity);
                if (values[i] is null)
                    return null;
            }

            return CreateKey(values, 0, values.Length);
        }

        private static object?[]? GetKeyValues(IReadOnlyList<Func<object, object?>> getters, object entity)
        {
            var values = new object?[getters.Count];
            for (var i = 0; i < getters.Count; i++)
            {
                values[i] = getters[i](entity);
                if (values[i] is null)
                    return null;
            }

            return values;
        }

        private static object? CreateKey(IReadOnlyList<object?> values, int offset, int count)
        {
            if (count == 1)
                return values[offset];

            var keyValues = new object?[count];
            for (var i = 0; i < count; i++)
            {
                keyValues[i] = values[offset + i];
                if (keyValues[i] is null)
                    return null;
            }

            return new JoinCompositeKey(keyValues);
        }

        private static object?[]? GetKeyValuesFromKey(object key, int expectedCount)
        {
            if (expectedCount == 1)
                return new object?[] { key };

            return key is JoinCompositeKey composite && composite.Count == expectedCount
                ? composite.ToArray()
                : null;
        }

        private sealed class JoinCompositeKey : IEquatable<JoinCompositeKey>
        {
            private readonly object?[] _values;

            public JoinCompositeKey(object?[] values) => _values = values;

            public int Count => _values.Length;

            public object?[] ToArray() => (object?[])_values.Clone();

            public bool Equals(JoinCompositeKey? other)
            {
                if (other is null || other._values.Length != _values.Length)
                    return false;

                for (var i = 0; i < _values.Length; i++)
                {
                    if (!object.Equals(_values[i], other._values[i]))
                        return false;
                }

                return true;
            }

            public override bool Equals(object? obj) => Equals(obj as JoinCompositeKey);

            public override int GetHashCode()
            {
                var hash = new HashCode();
                foreach (var value in _values)
                    hash.Add(value);
                return hash.ToHashCode();
            }

            public override string ToString()
                => string.Join("|", _values.Select(v => Convert.ToString(v, System.Globalization.CultureInfo.InvariantCulture)));
        }
    }
}
