using System;
using System.Linq.Expressions;

namespace nORM.Core
{
    public sealed class SetPropertyCalls<T>
    {
        public SetPropertyCalls<T> SetProperty<TProperty>(Expression<Func<T, TProperty>> property, TProperty value) => this;
    }
}
