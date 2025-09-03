using System;

namespace nORM.Query
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public sealed class SqlFunctionAttribute : Attribute
    {
        public SqlFunctionAttribute(string format)
        {
            Format = format;
        }

        public string Format { get; }
    }
}
