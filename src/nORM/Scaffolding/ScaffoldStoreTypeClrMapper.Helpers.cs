#nullable enable
using System;
using System.Text;

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
            => CollapseWhitespace(
                StripFacets(storeType.Trim().ToLowerInvariant())
                    .Replace('_', ' '));

        private static string CollapseWhitespace(string value)
        {
            var result = new StringBuilder(value.Length);
            var previousWasWhitespace = false;
            foreach (var ch in value)
            {
                if (char.IsWhiteSpace(ch))
                {
                    if (!previousWasWhitespace && result.Length > 0)
                    {
                        result.Append(' ');
                        previousWasWhitespace = true;
                    }

                    continue;
                }

                result.Append(ch);
                previousWasWhitespace = false;
            }

            return result.ToString().Trim();
        }

        private static string StripFacets(string storeType)
        {
            var paren = storeType.IndexOf('(');
            return paren >= 0 ? storeType[..paren].Trim() : storeType.Trim();
        }
    }
}
