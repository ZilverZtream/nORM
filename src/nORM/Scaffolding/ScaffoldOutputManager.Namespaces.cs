#nullable enable
using nORM.Core;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldOutputManager
    {
        public static string NormalizeContextNamespace(string entityNamespaceName, string? contextNamespaceName)
        {
            if (string.IsNullOrWhiteSpace(contextNamespaceName))
                return entityNamespaceName;

            var trimmed = contextNamespaceName.Trim();
            if (!ScaffoldNameHelper.IsValidNamespaceName(trimmed))
            {
                throw new NormConfigurationException(
                    $"Scaffold context namespace '{trimmed}' is not a valid C# namespace. " +
                    "Use a dot-separated namespace such as 'MyApp.Data.Contexts'.");
            }

            return trimmed;
        }
    }
}
