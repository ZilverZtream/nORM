namespace nORM.Configuration
{
    /// <summary>
    /// Controls optional provider-native tenant security integration.
    /// </summary>
    public enum NativeTenantSecurityMode
    {
        /// <summary>
        /// Do not set provider-native tenant session state.
        /// </summary>
        Disabled = 0,

        /// <summary>
        /// Set provider-native tenant session context before nORM commands run.
        /// Applications can bind database-native RLS policies to that session value.
        /// </summary>
        SessionContext = 1
    }
}
