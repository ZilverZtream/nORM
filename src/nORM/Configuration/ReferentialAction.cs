#nullable enable

namespace nORM.Configuration
{
    /// <summary>
    /// Describes the database referential action emitted for a foreign key in
    /// migration snapshots and provider DDL.
    /// </summary>
    public enum ReferentialAction
    {
        /// <summary>No explicit provider action; providers use their default no-action semantics.</summary>
        NoAction,

        /// <summary>Delete or update dependent rows when the principal row changes.</summary>
        Cascade,

        /// <summary>Set dependent foreign-key columns to <c>NULL</c>.</summary>
        SetNull,

        /// <summary>Restrict the principal change while dependents exist.</summary>
        Restrict,

        /// <summary>Set dependent foreign-key columns to their database defaults.</summary>
        SetDefault
    }
}
