using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;

#nullable enable

namespace nORM.Internal
{
    /// <summary>
    /// Provides optimized parameter binding with provider-aware type coercion.
    /// Expanded type map covering 33 types reduces fallback to DbType.Object
    /// which can cause inefficient query plans. Public to allow source-generated
    /// compile-time query methods to use the same parameter binding logic as
    /// runtime queries (SG1 fix).
    /// </summary>
    // PUBLIC BY DESIGN: the source generator (nORM.SourceGenerators.MaterializerQueryGenerator) emits
    // `nORM.Internal.ParameterOptimizer.AddOptimizedParam(...)` into the CONSUMER's assembly, which cannot
    // see nORM internals via InternalsVisibleTo. This type is therefore part of the stable code-gen contract
    // and must remain public. Enforced by CompileTimeQueryParameterParityTests.SG1_ParameterOptimizer_Type_Is_Public.
    public static class ParameterOptimizer
    {
        /// <summary>
        /// String parameters shorter than or equal to this threshold use their exact length
        /// as the <see cref="DbParameter.Size"/>. Longer strings use <c>-1</c> (unlimited)
        /// to avoid provider-specific truncation or buffer pre-allocation issues.
        /// 4000 matches the SQL Server NVARCHAR(MAX) threshold and is a common provider boundary.
        /// Note: Size is measured in UTF-16 code units (char count). Strings with surrogate pairs
        /// will have a Size slightly higher than Unicode character count, which is safe — all
        /// supported providers treat too-large Size as equivalent to unbounded.
        /// </summary>
        // internal so NormQueryProvider and source-generated code can share this value
        // without duplicating the magic number.
        internal const int MaxInlineStringSize = 4000;

        private static readonly ConcurrentDictionary<Type, DbType> _typeMap = new()
        {
            // Original 7 types
            [typeof(int)] = DbType.Int32,
            [typeof(long)] = DbType.Int64,
            [typeof(string)] = DbType.String,
            [typeof(DateTime)] = DbType.DateTime2,
            [typeof(bool)] = DbType.Boolean,
            [typeof(decimal)] = DbType.Decimal,
            [typeof(Guid)] = DbType.Guid,

            // Additional integer types
            [typeof(short)] = DbType.Int16,
            [typeof(byte)] = DbType.Byte,
            [typeof(sbyte)] = DbType.SByte,
            [typeof(ushort)] = DbType.UInt16,
            [typeof(uint)] = DbType.UInt32,
            [typeof(ulong)] = DbType.UInt64,

            // Additional floating point types
            [typeof(float)] = DbType.Single,
            [typeof(double)] = DbType.Double,

            // Date/time types
            [typeof(DateTimeOffset)] = DbType.DateTimeOffset,
            [typeof(TimeSpan)] = DbType.Time,

            // Additional date/time types (.NET 6+)
            [typeof(DateOnly)] = DbType.Date,
            [typeof(TimeOnly)] = DbType.Time,

            // Character type
            [typeof(char)] = DbType.StringFixedLength,

            // Binary data
            [typeof(byte[])] = DbType.Binary,

            // Nullable versions of common types
            [typeof(int?)] = DbType.Int32,
            [typeof(long?)] = DbType.Int64,
            [typeof(DateTime?)] = DbType.DateTime2,
            [typeof(bool?)] = DbType.Boolean,
            [typeof(decimal?)] = DbType.Decimal,
            [typeof(Guid?)] = DbType.Guid,
            [typeof(short?)] = DbType.Int16,
            [typeof(byte?)] = DbType.Byte,
            [typeof(float?)] = DbType.Single,
            [typeof(double?)] = DbType.Double,
            [typeof(DateTimeOffset?)] = DbType.DateTimeOffset,
            [typeof(TimeSpan?)] = DbType.Time,

            // Nullable versions of unsigned/signed integers and temporal types
            // Missing these causes null params to bind as DbType.Object on non-SQLite providers
            [typeof(sbyte?)] = DbType.SByte,
            [typeof(ushort?)] = DbType.UInt16,
            [typeof(uint?)] = DbType.UInt32,
            [typeof(ulong?)] = DbType.UInt64,
            [typeof(DateOnly?)] = DbType.Date,
            [typeof(TimeOnly?)] = DbType.Time,
            [typeof(char?)] = DbType.StringFixedLength
        };

        /// <summary>
        /// Adds a parameter to the command, attempting to infer the optimal <see cref="DbType"/> and
        /// size based on the supplied value. When a <paramref name="knownType"/> is provided and the
        /// value is <c>null</c>, the mapping is still applied to avoid provider ambiguity.
        /// </summary>
        /// <param name="cmd">The command to which the parameter is added.</param>
        /// <param name="name">The parameter name including prefix (e.g. <c>@Id</c>).</param>
        /// <param name="value">The value to bind to the parameter.</param>
        /// <param name="knownType">Optional type hint used when <paramref name="value"/> is <c>null</c>.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="name"/> is <c>null</c>.</exception>
        // PERFORMANCE OPTIMIZATION 21: Aggressive optimization for parameter creation hot path
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static void AddOptimizedParam(this DbCommand cmd, string name, object? value, Type? knownType = null)
        {
            // E-2: Guard against null name early; a null ParameterName produces cryptic
            // provider-specific exceptions (e.g. NullReferenceException inside the driver).
            ArgumentNullException.ThrowIfNull(name);

            var param = cmd.CreateParameter();
            param.ParameterName = name;

            // P1/X1: DBNull.Value is not reference-equal to null, so `value == null` does not catch it.
            // Callers use `paramValues[i] ?? DBNull.Value`, which means a null runtime argument arrives
            // here as DBNull.Value. Without this normalization DBNull falls into the non-null type-dispatch
            // branch where System.DBNull has no entry in _typeMap, leaving param.DbType at the ADO.NET
            // provider default (typically DbType.String) rather than DbType.Object — causing type-metadata
            // contamination and potential provider-specific binding errors for typed-null comparisons.
            if (value is DBNull) value = null;

            if (value == null)
            {
                param.Value = DBNull.Value;
                // For null parameters, apply the known DbType so providers can interpret
                // the null correctly (e.g., avoid "ambiguous column type" errors).
                //
                // N-1 fix: nullable enum unwrapping must handle two cases:
                //   1. Non-nullable enum: typeof(MyEnum).IsEnum == true → unwrap directly.
                //   2. Nullable enum:     typeof(MyEnum?) is Nullable<MyEnum>; IsEnum == false.
                //      Must strip the Nullable<> wrapper first, THEN check IsEnum.
                var lookupType = knownType;
                if (lookupType != null)
                {
                    // Strip Nullable<T> wrapper so both MyEnum and MyEnum? resolve the same way.
                    var nullableUnderlying = Nullable.GetUnderlyingType(lookupType);
                    if (nullableUnderlying != null)
                        lookupType = nullableUnderlying;

                    if (lookupType.IsEnum)
                        lookupType = Enum.GetUnderlyingType(lookupType);
                }
                if (lookupType == typeof(Guid) && param is SqliteParameter)
                {
                    param.DbType = DbType.String;
                    param.Size = 36;
                }
                else if (lookupType != null && _typeMap.TryGetValue(lookupType, out var dbType))
                {
                    param.DbType = dbType;
                    if (dbType == DbType.Decimal)
                        ApplyProviderDecimalMetadata(param, null);
                    else if (IsVariableLengthType(dbType))
                        param.Size = GetTypedNullSize(dbType);
                    // SqlCommand.Prepare requires an explicit scale for time/datetime2/
                    // datetimeoffset parameters, including typed nulls.
                    else if (param is SqlParameter sqlScaledNull
                        && dbType is DbType.Time or DbType.DateTime2 or DbType.DateTimeOffset)
                        sqlScaledNull.Scale = 7;
                }
                else
                {
                    param.DbType = DbType.Object;
                }
            }
            else
            {
                param.Value = value;
                var valueType = value.GetType();

                // PERFORMANCE OPTIMIZATION 23: Avoid TryGetValue overhead for most common types
                // Check common types directly first
                if (valueType == typeof(int))
                {
                    param.DbType = DbType.Int32;
                }
                else if (valueType == typeof(string))
                {
                    param.DbType = DbType.String;
                    if (param is SqlParameter)
                    {
                        // SqlCommand.Prepare rejects variable-length parameters with
                        // Size 0, so an empty string must still declare a positive
                        // size or every prepared batch containing one throws.
                        var str = (string)value;
                        param.Size = str.Length == 0 ? 1
                            : str.Length <= MaxInlineStringSize ? str.Length : -1;
                    }
                }
                else if (valueType == typeof(long))
                {
                    param.DbType = DbType.Int64;
                }
                else if (valueType == typeof(ulong))
                {
                    // See ParameterAssign.ToStorableInt64: ulong has no portable native representation,
                    // so it is stored as signed 64-bit. Values in range map identically; a value above
                    // long.MaxValue fails loud rather than wrap to a negative bit pattern.
                    param.Value = nORM.Query.ParameterAssign.ToStorableInt64((ulong)value);
                    param.DbType = DbType.Int64;
                }
                else if (valueType == typeof(bool))
                {
                    param.DbType = DbType.Boolean;
                }
                else if (valueType == typeof(decimal))
                {
                    param.DbType = DbType.Decimal;
                    ApplyProviderDecimalMetadata(param, value);
                }
                else if (valueType == typeof(DateTime))
                {
                    param.DbType = DbType.DateTime2;
                    param.Value = DateTime.SpecifyKind((DateTime)value, DateTimeKind.Unspecified);
                    // SqlCommand.Prepare requires an explicit scale for datetime2
                    // parameters (it misreports the omission as a variable-length
                    // Size error). 7 is datetime2's full 100ns precision.
                    if (param is SqlParameter sqlDt)
                        sqlDt.Scale = 7;
                }
                else if (valueType == typeof(DateOnly))
                {
                    // Bind DateOnly as its canonical `yyyy-MM-dd` text form. ADO.NET providers
                    // that store dates as TEXT (SQLite, MySQL CHAR/VARCHAR) compare directly
                    // against this representation; the ones with first-class DATE columns
                    // (SQL Server, PostgreSQL) accept the ISO string at the binding boundary and
                    // narrow it to their native date type without truncating the time component.
                    // Converting to DateTime(midnight) used to produce `'2024-06-30 00:00:00'`
                    // which never equality-matched TEXT-stored `'2024-06-30'`.
                    var d = (DateOnly)value;
                    param.Value = d.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
                    param.DbType = DbType.String;
                }
                else if (valueType == typeof(TimeOnly))
                {
                    // P1: TimeOnly requires explicit conversion to TimeSpan for most providers
                    var t = (TimeOnly)value;
                    param.Value = t.ToTimeSpan();
                    param.DbType = DbType.Time;
                    // See the DateTime branch: Prepare needs an explicit scale for time.
                    if (param is SqlParameter sqlTime)
                        sqlTime.Scale = 7;
                }
                else if (valueType == typeof(char))
                {
                    // P1: Char must be converted to string for correct provider binding.
                    // T-2/N-4: Set Size=1 explicitly so providers that size fixed-char columns
                    // by the Size field (e.g. NCHAR(n)) get the correct single-character width.
                    param.Value = value.ToString();
                    param.DbType = DbType.StringFixedLength;
                    param.Size = 1;
                }
                else if (valueType == typeof(Guid) && param is SqliteParameter)
                {
                    // SQLite stores nORM Guid columns as canonical TEXT. Bind the same
                    // representation so tenant predicates and equality filters compare
                    // against the stored value instead of provider-specific Guid/BLOB data.
                    param.Value = ((Guid)value).ToString("D", System.Globalization.CultureInfo.InvariantCulture);
                    param.DbType = DbType.String;
                    param.Size = 36;
                }
                else if (valueType.IsEnum)
                {
                    // Enums must be converted to their underlying integral type to avoid
                    // provider-specific ToString() coercion issues.
                    //
                    // E-1 fix: wrap Convert.ChangeType with a descriptive exception so callers
                    // get actionable context (parameter name, enum type, underlying type) instead
                    // of a bare InvalidCastException/OverflowException from deep inside the driver.
                    //
                    // C-3 fix: explicitly set DbType.Object when the underlying type is not in
                    // _typeMap, rather than leaving param.DbType at whatever the ADO.NET provider
                    // default is for a freshly created parameter (provider-specific, undefined).
                    var underlying = Enum.GetUnderlyingType(valueType);
                    try
                    {
                        param.Value = Convert.ChangeType(value, underlying);
                    }
                    catch (Exception ex) when (ex is InvalidCastException or OverflowException)
                    {
                        throw new InvalidOperationException(
                            $"Failed to convert enum value '{value}' of type '{valueType.Name}' " +
                            $"to its underlying type '{underlying.Name}' for parameter '{name}'.", ex);
                    }
                    if (!_typeMap.TryGetValue(underlying, out var enumDbType))
                        enumDbType = DbType.Object;
                    param.DbType = enumDbType;
                    if (underlying == typeof(ulong))
                    {
                        // ulong-backed enum: store as signed 64-bit with the same range guard.
                        param.Value = nORM.Query.ParameterAssign.ToStorableInt64((ulong)param.Value!);
                        param.DbType = DbType.Int64;
                    }
                }
                else if (_typeMap.TryGetValue(valueType, out var mappedType))
                {
                    param.DbType = mappedType;
                    // byte[] parameters need Size=-1 on most providers (SQL Server, PostgreSQL,
                    // MySQL) to allow MAX-length binaries. Without this the provider may default
                    // to Size=0 or a small driver default that truncates large VARBINARY/BYTEA values.
                    // D-3 fix: broadened comment — this applies to all providers, not just SQL Server.
                    if (valueType == typeof(byte[]))
                        param.Size = -1;
                    // SqlCommand.Prepare requires an explicit scale for time/datetime2/
                    // datetimeoffset parameters (misreported as a Size error); 7 is the
                    // full 100ns precision of those types.
                    else if (param is SqlParameter sqlScaled
                        && mappedType is DbType.Time or DbType.DateTime2 or DbType.DateTimeOffset)
                        sqlScaled.Scale = 7;
                }
                else
                {
                    // FO-1: Unknown non-null types fall back to DbType.Object so the provider
                    // uses its own type inference rather than inheriting whatever default the
                    // freshly-created DbParameter happens to have. Mirrors the null-branch
                    // fallback and the ParameterAssign.AssignValue fallback for consistency.
                    param.DbType = DbType.Object;
                }
            }

            cmd.Parameters.Add(param);
        }

        private static bool IsVariableLengthType(DbType dbType)
            => dbType is DbType.String
                or DbType.AnsiString
                or DbType.StringFixedLength
                or DbType.AnsiStringFixedLength
                or DbType.Binary;

        private static int GetTypedNullSize(DbType dbType)
            => dbType == DbType.Binary ? -1 : MaxInlineStringSize;

        internal static void ApplyProviderDecimalMetadata(DbParameter param, object? value)
        {
            if (param is not SqlParameter sqlParameter)
                return;

            sqlParameter.Precision = 29;
            sqlParameter.Scale = value is decimal decimalValue
                ? GetDecimalScale(decimalValue)
                : (byte)10;
        }

        private static byte GetDecimalScale(decimal value)
        {
            var bits = decimal.GetBits(value);
            return (byte)((bits[3] >> 16) & 0x7F);
        }

        /// <summary>
        /// Adds a parameter to the command, inferring the <see cref="DbType"/> from the runtime
        /// type of <paramref name="value"/>. Equivalent to calling
        /// <see cref="AddOptimizedParam(DbCommand,string,object?,Type?)"/> with no <c>knownType</c>.
        /// For typed-null parameters (value is <c>null</c> but the column type is known), prefer
        /// <see cref="AddOptimizedParam(DbCommand,string,object?,Type?)"/> with an explicit type hint
        /// to avoid provider ambiguity on the null binding. (A-1 fix: corrected misleading doc that
        /// said "without additional type metadata" — type IS inferred from value.GetType().)
        /// </summary>
        /// <param name="cmd">The command to which the parameter is added.</param>
        /// <param name="name">The parameter name including prefix.</param>
        /// <param name="value">The value to bind to the parameter.</param>
        public static void AddParam(this DbCommand cmd, string name, object? value)
            => AddOptimizedParam(cmd, name, value, null);
    }
}
