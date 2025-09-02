using System;

#nullable enable

namespace nORM.Core
{
    public enum EntityState
    {
        Detached,
        Unchanged,
        Added,
        Modified,
        Deleted
    }
}
