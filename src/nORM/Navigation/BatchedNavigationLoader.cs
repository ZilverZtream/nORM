using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

namespace nORM.Navigation
{
    public sealed class BatchedNavigationLoader : IDisposable
    {
        private readonly DbContext _context;
        private readonly Dictionary<(Type EntityType, string PropertyName), List<(object Entity, TaskCompletionSource<object> Tcs)>> _pendingLoads = new();
        private readonly Timer _batchTimer;
        private readonly SemaphoreSlim _batchSemaphore = new(1, 1);

        public BatchedNavigationLoader(DbContext context)
        {
            _context = context;
            _batchTimer = new Timer(ProcessBatch, null, TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(10));
            _context.RegisterForDisposal(this);
        }

        public async Task<List<object>> LoadNavigationAsync(object entity, string propertyName, CancellationToken ct = default)
        {
            var entityType = entity.GetType();
            var key = (entityType, propertyName);
            var tcs = new TaskCompletionSource<object>();

            await _batchSemaphore.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (!_pendingLoads.TryGetValue(key, out var list))
                {
                    list = new List<(object, TaskCompletionSource<object>)>();
                    _pendingLoads[key] = list;
                }
                list.Add((entity, tcs));
            }
            finally
            {
                _batchSemaphore.Release();
            }

            return (List<object>)await tcs.Task.ConfigureAwait(false);
        }

        private async void ProcessBatch(object? state)
        {
            if (!await _batchSemaphore.WaitAsync(1).ConfigureAwait(false))
                return;

            try
            {
                var batches = new Dictionary<(Type, string), List<(object, TaskCompletionSource<object>)>>(_pendingLoads);
                _pendingLoads.Clear();

                foreach (var kvp in batches)
                {
                    var (entityType, propertyName) = kvp.Key;
                    var entities = kvp.Value;

                    await LoadNavigationBatchAsync(entityType, propertyName, entities).ConfigureAwait(false);
                }
            }
            finally
            {
                _batchSemaphore.Release();
            }
        }

        private async Task LoadNavigationBatchAsync(Type entityType, string propertyName,
            List<(object Entity, TaskCompletionSource<object> Tcs)> entities)
        {
            try
            {
                var mapping = _context.GetMapping(entityType);
                if (!mapping.Relations.TryGetValue(propertyName, out var relation))
                    return;

                var keys = entities.Select(e => relation.PrincipalKey.Getter(e.Entity))
                                   .Where(k => k != null)
                                   .Distinct()
                                   .ToList();

                if (!keys.Any())
                    return;

                var relatedData = await LoadRelatedDataBatch(relation, keys).ConfigureAwait(false);
                var grouped = relatedData.GroupBy(relation.ForeignKey.Getter)
                                         .ToDictionary(g => g.Key!, g => g.ToList());

                foreach (var (entity, tcs) in entities)
                {
                    var key = relation.PrincipalKey.Getter(entity);
                    var related = grouped.TryGetValue(key!, out var list) ? list : new List<object>();
                    tcs.SetResult(related);
                }
            }
            catch (Exception ex)
            {
                foreach (var (_, tcs) in entities)
                    tcs.SetException(ex);
            }
        }

        private async Task<List<object>> LoadRelatedDataBatch(TableMapping.Relation relation, List<object?> keys)
        {
            var mapping = _context.GetMapping(relation.DependentType);
            await _context.EnsureConnectionAsync(default).ConfigureAwait(false);
            using var cmd = _context.Connection.CreateCommand();
            cmd.CommandTimeout = (int)_context.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;

            var paramNames = new List<string>();
            for (int i = 0; i < keys.Count; i++)
            {
                var pn = _context.Provider.ParamPrefix + "p" + i;
                paramNames.Add(pn);
                cmd.AddParam(pn, keys[i]);
            }

            cmd.CommandText = $"SELECT * FROM {mapping.EscTable} WHERE {relation.ForeignKey.EscCol} IN ({string.Join(",", paramNames)})";

            var materializer = Query.QueryTranslator.Rent(_context).CreateMaterializer(mapping, relation.DependentType);
            var results = new List<object>();

            using var reader = await cmd.ExecuteReaderWithInterceptionAsync(_context, CommandBehavior.Default, default).ConfigureAwait(false);
            while (await reader.ReadAsync(default).ConfigureAwait(false))
            {
                var entity = await materializer(reader, default).ConfigureAwait(false);
                var entry = _context.ChangeTracker.Track(entity, EntityState.Unchanged, mapping);
                entity = entry.Entity;
                NavigationPropertyExtensions._navigationContexts.GetValue(entity, _ => new NavigationContext(_context, relation.DependentType));
                results.Add(entity);
            }
            return results;
        }

        public void Dispose()
        {
            _batchTimer.Dispose();
            _batchSemaphore.Dispose();
        }
    }
}
