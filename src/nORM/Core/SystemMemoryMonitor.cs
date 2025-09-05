using System;

namespace nORM.Core
{
    /// <summary>
    /// Default implementation of <see cref="IMemoryMonitor"/> that queries the runtime for memory information.
    /// </summary>
    internal sealed class SystemMemoryMonitor : IMemoryMonitor
    {
        public long GetAvailableMemory()
        {
            return GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        }
    }
}
