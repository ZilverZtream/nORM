using System;
using System.Collections.Generic;
using nORM.Providers;

#nullable enable

namespace nORM.Configuration
{
    public static partial class ProviderMobilityTranslator
    {
        private static void AddSqlShapeImplementationDecisions(
            List<ProviderMobilityProviderDecision> decisions,
            DatabaseProvider provider,
            ProviderCapabilities capabilities,
            Version? actualServerVersion)
        {
            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.IdentifierEscaping,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                $"{capabilities.ProviderName} identifiers are escaped through the concrete nORM provider strategy ({provider.Escape("NormProbe")}).",
                "Keep generated SQL paths on nORM identifier escaping and avoid caller-authored delimited SQL.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.ParameterBinding,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                $"{capabilities.ProviderName} parameters use the '{provider.ParamPrefix}' prefix and provider-owned DbParameter creation.",
                "Use generated nORM query/write APIs so parameter names and provider parameter objects stay owned by nORM.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.PagingTranslation,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                provider.UsesFetchOffsetPaging
                    ? $"{capabilities.ProviderName} uses SQL Server-style OFFSET/FETCH paging translation."
                    : $"{capabilities.ProviderName} uses LIMIT/OFFSET paging translation.",
                "Keep OrderBy/Skip/Take live-provider parity tests current for this target.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.BooleanPredicateTranslation,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                provider.PrefersBareBooleanPredicates
                    ? $"{capabilities.ProviderName} prefers bare boolean predicates for generated WHERE clauses."
                    : $"{capabilities.ProviderName} translates boolean predicates to provider boolean literals.",
                "Keep boolean predicate translation in generated LINQ; avoid raw provider boolean fragments.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.NullSemantics,
                IsExpandedNullSemantics(provider.NullSafeEqual("a", "b")) ? ProviderMobilitySupport.Emulated : ProviderMobilitySupport.Portable,
                true,
                "Info",
                IsExpandedNullSemantics(provider.NullSafeEqual("a", "b"))
                    ? $"{capabilities.ProviderName} null-safe equality is preserved by generated SQL expansion ({provider.NullSafeEqual("a", "b")})."
                    : $"{capabilities.ProviderName} uses native null-safe equality syntax ({provider.NullSafeEqual("a", "b")}).",
                "Keep equality/inequality null-semantics parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.LikeEscapeTranslation,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                $"{capabilities.ProviderName} LIKE wildcard escaping uses '{provider.LikeEscapeChar}' and provider-owned REPLACE translation.",
                "Keep StartsWith/EndsWith/Contains LIKE escaping covered by provider parity tests.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, PD(
                capabilities.ProviderName,
                ProviderMobilityProviderFeature.StringConcatTranslation,
                ProviderMobilitySupport.Portable,
                true,
                "Info",
                $"{capabilities.ProviderName} string concatenation is translated as {provider.GetConcatSql("left", "right")}.",
                "Keep string concatenation and string interpolation query parity tests current.",
                capabilities.MinimumServerVersion,
                actualServerVersion));

            Replace(decisions, BuildTypeConversionDecision(provider, actualServerVersion));
            Replace(decisions, BuildStringPredicateDecision(provider, actualServerVersion));
            Replace(decisions, BuildCharacterTranslationDecision(provider, actualServerVersion));
            Replace(decisions, BuildFormattingTranslationDecision(provider, actualServerVersion));
        }
    }
}
