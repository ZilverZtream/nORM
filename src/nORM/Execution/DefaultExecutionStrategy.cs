using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;

#nullable enable

namespace nORM.Execution
{
    internal sealed class DefaultExecutionStrategy : IExecutionStrategy
    {
        private readonly DbContext _ctx;
        
        public DefaultExecutionStrategy(DbContext ctx) => _ctx = ctx;
        
        public async Task<T> ExecuteAsync<T>(Func<DbContext, CancellationToken, Task<T>> operation, CancellationToken ct)
        {
            try
            {
                return await operation(_ctx, ct);
            }
            catch (DbException ex)
            {
                throw new NormException(ex.Message, null, null, ex);
            }
        }
    }
}