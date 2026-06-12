using System;

#nullable enable

namespace nORM.Providers
{
    public partial class SqliteProvider
    {
        /// <summary>
        /// Attempts to translate a .NET method into its SQLite SQL equivalent.
        /// </summary>
        /// <param name="name">Name of the method being translated.</param>
        /// <param name="declaringType">Type that declares the method.</param>
        /// <param name="args">SQL fragments representing the arguments.</param>
        /// <returns>The translated SQL or <c>null</c> if unsupported.</returns>
        public override string? TranslateFunction(string name, Type declaringType, params string[] args)
        {
            return TryTranslatePrimitiveCompareFunction(name, declaringType, args)
                ?? TryTranslateStringFunction(name, declaringType, args)
                ?? TryTranslateDateTimeFunction(name, declaringType, args)
                ?? TryTranslateDateOnlyFunction(name, declaringType, args)
                ?? TryTranslateTimeSpanFunction(name, declaringType, args)
                ?? TryTranslateEnumFunction(name, declaringType, args)
                ?? TryTranslateCharFunction(name, declaringType, args)
                ?? TryTranslateTimeOnlyFunction(name, declaringType, args)
                ?? TryTranslateParseAndGuidFunction(name, declaringType, args)
                ?? TryTranslateNormFunction(name, declaringType, args)
                ?? TryTranslateIeee754Function(name, declaringType, args)
                ?? TryTranslateMathFunction(name, declaringType, args)
                ?? TryTranslateDecimalFunction(name, declaringType, args)
                ?? TryTranslateConvertFunction(name, declaringType, args);
        }
    }
}
