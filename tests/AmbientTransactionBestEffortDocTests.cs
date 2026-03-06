using Xunit;

namespace nORM.Tests;

/// <summary>
/// T-1: Documentation artifact for BestEffort ambient transaction behavior.
///
/// Known behavior: when a TransactionScope uses TransactionScopeOption.Required
/// and the underlying ADO.NET driver does not support full distributed transaction
/// enlistment (e.g., SQLite in-memory), nORM uses the BestEffort enlistment policy.
/// Under BestEffort, each database operation within the scope may commit independently
/// rather than participating in a single atomic two-phase commit. This means that if
/// the TransactionScope is rolled back or not completed, operations already committed
/// to the database may NOT be rolled back.
///
/// This is a design-level trade-off for drivers with limited distributed-transaction
/// support. Applications using SQLite in production (unusual) or drivers without
/// DTC support should avoid ambient TransactionScope for atomicity guarantees and
/// use explicit DbTransaction instead.
///
/// This test is intentionally a no-op assertion — it exists solely to document
/// the known behavior and prevent future regressions from being misclassified as
/// new bugs.
/// </summary>
public class AmbientTransactionBestEffortDocTests
{
    [Fact]
    public void BestEffort_AmbientTransaction_DocumentedBehavior()
    {
        // T-1: This is a documentation artifact, not a behavioral gate.
        // See class-level XML summary for the documented known behavior.
        Assert.True(true);
    }
}
