namespace nORM.Core
{
    /// <summary>
    /// Specifies how entities returned from queries are tracked by the <see cref="DbContext"/>.
    /// </summary>
    public enum QueryTrackingBehavior
    {
        /// <summary>
        /// Track all entities so that changes can be detected and persisted.
        /// </summary>
        TrackAll,

        /// <summary>
        /// Do not track entities; queries return detached objects.
        /// </summary>
        NoTracking
    }
}
