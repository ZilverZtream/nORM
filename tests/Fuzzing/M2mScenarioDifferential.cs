using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;

#nullable enable

namespace nORM.Tests.Fuzzing
{
    /// <summary>
    /// The many-to-many slice of the write-path snapshot-diff oracle. A scenario loads Posts with their Tags
    /// collection (via Include), edits the tracked collection (Link/Unlink), and saves; a reference set of
    /// (postId, tagId) links tracks the intended association set. After the scenario the AUTHORITATIVE join-table
    /// state (raw SQL, not the tracker) must equal the reference set — a dropped add, dropped remove, or duplicate
    /// join row is a <see cref="FuzzOutcome.WrongResult"/>. This is the m2m_write_sync silent-no-op area.
    /// Scenarios are constrained so the oracle is unambiguous: a post is edited only after being loaded, a Link
    /// targets a pair not currently linked, and an Unlink targets a pair that is.
    /// </summary>
    public static class M2mScenarioDifferential
    {
        public const string Family = "m2m";
        public const int GeneratorVersion = 1;

        public static FuzzCaseResult Execute(M2mScenario scenario, long seed)
        {
            var serialized = scenario.ToJson();
            var model = new HashSet<(int Post, int Tag)>(scenario.InitialLinks.Select(l => (l.PostId, l.TagId)));

            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            Seed(cn, scenario);

            try
            {
                using var ctx = NewCtx(cn);
                var tags = ctx.Query<M2mfTag>().ToList().ToDictionary(t => t.Id);
                List<M2mfPost>? loadedPosts = null;

                M2mfPost Post(int id)
                {
                    loadedPosts ??= ((INormQueryable<M2mfPost>)ctx.Query<M2mfPost>()).Include(p => p.Tags).ToList();
                    return loadedPosts.Single(p => p.Id == id);
                }

                foreach (var op in scenario.Ops)
                {
                    switch (op.Kind)
                    {
                        case M2mOpKind.LoadPost:
                            _ = Post(op.PostId);   // ensure the collection is loaded (snapshot captured)
                            break;
                        case M2mOpKind.Link:
                            Post(op.PostId).Tags.Add(tags[op.TagId]);
                            model.Add((op.PostId, op.TagId));
                            break;
                        case M2mOpKind.Unlink:
                            var post = Post(op.PostId);
                            var tag = post.Tags.First(t => t.Id == op.TagId);
                            post.Tags.Remove(tag);
                            model.Remove((op.PostId, op.TagId));
                            break;
                        case M2mOpKind.Save:
                            ctx.SaveChangesAsync().GetAwaiter().GetResult();
                            break;
                    }
                }

                // Flush any trailing pending edits.
                ctx.SaveChangesAsync().GetAwaiter().GetResult();
            }
            catch (NormUnsupportedFeatureException nufe)
            {
                return new FuzzCaseResult
                {
                    Family = Family, Seed = seed, GeneratorVersion = GeneratorVersion,
                    Outcome = FuzzOutcome.UnexpectedlyRejected,
                    ReasonCode = "m2m/" + nufe.GetType().Name,
                    SerializedCase = serialized, Detail = nufe.Message, Features = ExtractFeatures(scenario),
                };
            }
            catch (Exception ex)
            {
                return Fail(FuzzOutcome.UnexpectedException, $"{ex.GetType().Name}: {ex.Message}", scenario, seed, serialized);
            }

            // Compare the authoritative join table to the reference link set.
            var db = new HashSet<(int, int)>();
            using (var read = cn.CreateCommand())
            {
                read.CommandText = "SELECT PostId, TagId FROM M2mfPostTag";
                using var r = read.ExecuteReader();
                while (r.Read())
                {
                    var link = (r.GetInt32(0), r.GetInt32(1));
                    if (!db.Add(link))
                        return Fail(FuzzOutcome.WrongResult, $"duplicate join row {link.Item1}-{link.Item2}", scenario, seed, serialized);
                }
            }

            if (!db.SetEquals(model))
                return Fail(FuzzOutcome.WrongResult, $"db={Render(db)} model={Render(model)}", scenario, seed, serialized);

            return new FuzzCaseResult
            {
                Family = Family, Seed = seed, GeneratorVersion = GeneratorVersion,
                Outcome = FuzzOutcome.Executed, SerializedCase = serialized, Features = ExtractFeatures(scenario),
            };
        }

        private static void Seed(SqliteConnection cn, M2mScenario s)
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText =
                "CREATE TABLE M2mfPost (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);" +
                "CREATE TABLE M2mfTag (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL);" +
                "CREATE TABLE M2mfPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
            foreach (var id in s.PostIds) Exec(cn, $"INSERT INTO M2mfPost VALUES ({id}, 'p{id}')");
            foreach (var id in s.TagIds) Exec(cn, $"INSERT INTO M2mfTag VALUES ({id}, 't{id}')");
            foreach (var l in s.InitialLinks) Exec(cn, $"INSERT INTO M2mfPostTag VALUES ({l.PostId}, {l.TagId})");
        }

        private static void Exec(SqliteConnection cn, string sql)
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        private static DbContext NewCtx(SqliteConnection cn) => new(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<M2mfPost>().HasMany<M2mfTag>(p => p.Tags).WithMany().UsingTable("M2mfPostTag", "PostId", "TagId"),
        }, ownsConnection: false);

        private static string Render(HashSet<(int, int)> links)
            => "{" + string.Join(",", links.OrderBy(l => l.Item1).ThenBy(l => l.Item2).Select(l => $"{l.Item1}-{l.Item2}")) + "}";

        public static IReadOnlyList<string> ExtractFeatures(M2mScenario s)
        {
            var f = new SortedSet<string>(StringComparer.Ordinal);
            var kinds = s.Ops.Select(o => o.Kind).ToHashSet();
            if (kinds.Contains(M2mOpKind.Link)) f.Add("link");
            if (kinds.Contains(M2mOpKind.Unlink)) f.Add("unlink");
            if (s.InitialLinks.Count > 0) f.Add("initial-links");
            if (s.Ops.Count(o => o.Kind == M2mOpKind.Save) >= 2) f.Add("multi-save");
            if (s.Ops.Where(o => o.Kind == M2mOpKind.LoadPost).Select(o => o.PostId).Distinct().Count() >= 2) f.Add("multi-post");
            // A link added and then removed (or vice versa) on the same pair within one save = a net-change edit.
            for (var i = 0; i < s.Ops.Count; i++)
            {
                if (s.Ops[i].Kind is not (M2mOpKind.Link or M2mOpKind.Unlink)) continue;
                for (var j = i + 1; j < s.Ops.Count && s.Ops[j].Kind != M2mOpKind.Save; j++)
                    if (s.Ops[j].Kind is M2mOpKind.Link or M2mOpKind.Unlink
                        && s.Ops[j].PostId == s.Ops[i].PostId && s.Ops[j].TagId == s.Ops[i].TagId)
                    { f.Add("toggle-same-link-in-batch"); break; }
            }
            return f.ToArray();
        }

        private static FuzzCaseResult Fail(FuzzOutcome outcome, string detail, M2mScenario s, long seed, string serialized) => new()
        {
            Family = Family, Seed = seed, GeneratorVersion = GeneratorVersion,
            Outcome = outcome, SerializedCase = serialized, Detail = detail, Features = ExtractFeatures(s),
        };
    }
}
