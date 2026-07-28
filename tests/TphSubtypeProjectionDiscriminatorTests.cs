using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests
{
    /// <summary>
    /// A TPH subtype-root query with a Select projection (`Query&lt;Car&gt;().Select(c =&gt; c.Id)`) must still apply the
    /// discriminator filter. TranslationBuilder.Setup() injected the discriminator only when _rootType carried
    /// [DiscriminatorValue], but a projection changes _rootType to the projection's element type (int/anon), so
    /// nothing was injected and every sibling subtype's rows silently leaked into results, counts, sums, and
    /// quantifiers. Uses the shared Vehicle/Car/Truck integer-discriminator hierarchy.
    /// </summary>
    [Xunit.Trait("Category", "Fast")]
    public class TphSubtypeProjectionDiscriminatorTests
    {
        private static DbContext Ctx()
        {
            var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using (var cmd = cn.CreateCommand())
            {
                // One Car (Kind=1, Id=10) and one Truck (Kind=2, Id=20).
                cmd.CommandText = "CREATE TABLE Vehicle(Id INTEGER, Kind INTEGER, Doors INTEGER, Axles INTEGER);" +
                                  "INSERT INTO Vehicle VALUES(10,1,4,NULL);" +
                                  "INSERT INTO Vehicle VALUES(20,2,NULL,6);";
                cmd.ExecuteNonQuery();
            }
            return new DbContext(cn, new SqliteProvider());
        }

        [Fact]
        public void Projected_ToList_only_returns_the_subtype()
        {
            using var ctx = Ctx();
            var ids = ctx.Query<Car>().Select(c => c.Id).OrderBy(x => x).ToList();
            Assert.Equal(new[] { 10 }, ids);   // only the Car; the Truck (Id=20) must not leak
        }

        [Fact]
        public void Projected_Count_only_counts_the_subtype()
        {
            using var ctx = Ctx();
            Assert.Equal(1, ctx.Query<Car>().Select(c => c.Id).Count());
        }

        [Fact]
        public void Projected_Sum_only_sums_the_subtype()
        {
            using var ctx = Ctx();
            Assert.Equal(10, ctx.Query<Car>().Select(c => c.Id).Sum());
        }

        [Fact]
        public void Projected_Any_predicate_respects_the_subtype()
        {
            using var ctx = Ctx();
            // Id=20 is the Truck; a Car query must not see it.
            Assert.False(ctx.Query<Car>().Select(c => c.Id).Any(id => id == 20));
            Assert.True(ctx.Query<Car>().Select(c => c.Id).Any(id => id == 10));
        }

        [Fact]
        public void Projected_Contains_respects_the_subtype()
        {
            using var ctx = Ctx();
            Assert.False(ctx.Query<Car>().Select(c => c.Id).Contains(20));
            Assert.True(ctx.Query<Car>().Select(c => c.Id).Contains(10));
        }

        [Fact]
        public void Anonymous_projection_respects_the_subtype()
        {
            using var ctx = Ctx();
            var rows = ctx.Query<Car>().Select(c => new { c.Id, c.Doors }).OrderBy(x => x.Id).ToList();
            Assert.Single(rows);
            Assert.Equal(10, rows[0].Id);
            Assert.Equal(4, rows[0].Doors);
        }

        [Fact]
        public void Bare_subtype_Any_and_Count_still_work()
        {
            using var ctx = Ctx();
            // Finding A coupling: a bare subtype root Any()/Count() must not emit a double-WHERE.
            Assert.True(ctx.Query<Car>().Any());
            Assert.Equal(1, ctx.Query<Car>().Count());
        }
    }
}
