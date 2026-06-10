using System;

namespace nORM.Tests;

/// <summary>
/// Centralized runtime guard for live-provider tests that cannot execute without
/// a configured server. The release gate enforces required live-provider counts
/// before running live/RC modes, so local no-provider runs early-return here
/// instead of throwing framework-specific runtime skip exceptions.
/// </summary>
internal static class Skip
{
    public static bool If(bool condition, string reason)
    {
        if (condition)
        {
            Console.WriteLine($"[live-provider parity] skipped: {reason}");
            return true;
        }

        return false;
    }

    public static bool If(object? value, string reason) => If(value is null, reason);
}
