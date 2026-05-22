using System;
using System.Collections;
using System.Collections.Generic;
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

        /// <summary>Escaped join table name for use in SQL statements.</summary>
        public string EscTableName { get; }

        /// <summary>Escaped column in the join table that holds the FK to the left (owner) entity's PK.</summary>
        public string EscLeftFkColumn { get; }

        /// <summary>Escaped column in the join table that holds the FK to the right (related) entity's PK.</summary>
        public string EscRightFkColumn { get; }

        /// <summary>Plain left FK column name.</summary>
        public string LeftFkColumn { get; }

        /// <summary>Plain right FK column name.</summary>
        public string RightFkColumn { get; }

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
        {
            TableName = tableName;
            EscTableName = provider.Escape(tableName);
            LeftFkColumn = leftFkColumn;
            RightFkColumn = rightFkColumn;
            EscLeftFkColumn = provider.Escape(leftFkColumn);
            EscRightFkColumn = provider.Escape(rightFkColumn);
            LeftType = leftType;
            RightType = rightType;
            LeftNavPropertyName = leftNavPropertyName;
            RightNavPropertyName = rightNavPropertyName;

            LeftPkGetter = leftPkColumn.Getter;
            RightPkGetter = rightPkColumn.Getter;

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
    }
}
