#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldStoreTypeClrMapper
    {
        private static bool TryMapExact(string normalized, out Type clrType, params (string StoreType, Type ClrType)[] mappings)
        {
            var baseType = StripFacets(normalized);
            foreach (var (storeType, mappedType) in mappings)
            {
                if (string.Equals(baseType, storeType, StringComparison.Ordinal))
                {
                    clrType = mappedType;
                    return true;
                }
            }

            clrType = typeof(object);
            return false;
        }

        private static string Normalize(string storeType)
            => StripFacets(storeType.Trim().ToLowerInvariant())
                .Replace('_', ' ')
                .Replace("  ", " ", StringComparison.Ordinal)
                .Trim();

        private static string StripFacets(string storeType)
        {
            var paren = storeType.IndexOf('(');
            return paren >= 0 ? storeType[..paren].Trim() : storeType.Trim();
        }
    }
}
