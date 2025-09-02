using System;

namespace nORM.Mapping
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class TableSplitAttribute : Attribute
    {
        public Type PrincipalType { get; }

        public TableSplitAttribute(Type principalType)
        {
            PrincipalType = principalType;
        }
    }
}
