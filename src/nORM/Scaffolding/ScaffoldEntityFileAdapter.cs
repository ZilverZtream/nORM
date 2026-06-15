#nullable enable
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntityFileAdapter
    {
        public static async Task<ScaffoldEntityFileSet> BuildScaffoldEntityFilesAsync(
            ScaffoldEntityFileSetRequest request)
            => await ScaffoldEntityFileSetBuilder.BuildAsync(request).ConfigureAwait(false);

        public static Task<string> ScaffoldEntityAsync(ScaffoldEntitySourceInfo entity)
            => ScaffoldEntitySourceBuilder.BuildAsync(entity);
    }
}
