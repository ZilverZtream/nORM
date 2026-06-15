#nullable enable

using System;
using System.Collections.Generic;
using nORM.Configuration;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    [Fact]
    public void UniqueIndexShape_DistinguishesColumnSetFromOrderedAlternateKey()
    {
        var indexes = new[]
        {
            new ScaffoldIndexInfo(
                "Order",
                "ExternalNo",
                "UX_Order_ExternalNo_TenantId",
                IsUnique: true,
                ColumnCount: 2,
                Ordinal: 0,
                IsDescending: false,
                IsIncluded: false,
                NullSortOrder: IndexNullSortOrder.Default,
                NullsNotDistinct: false,
                FilterSql: null),
            new ScaffoldIndexInfo(
                "Order",
                "TenantId",
                "UX_Order_ExternalNo_TenantId",
                IsUnique: true,
                ColumnCount: 2,
                Ordinal: 1,
                IsDescending: false,
                IsIncluded: false,
                NullSortOrder: IndexNullSortOrder.Default,
                NullsNotDistinct: false,
                FilterSql: null)
        };

        Assert.True(ScaffoldForeignKeyShape.HasExactUniqueColumnSet(
            indexes,
            "Order",
            new HashSet<string>(new[] { "TenantId", "ExternalNo" }, StringComparer.OrdinalIgnoreCase)));
        Assert.False(ScaffoldForeignKeyShape.HasExactOrderedUniqueIndex(
            indexes,
            "Order",
            new[] { "TenantId", "ExternalNo" }));
        Assert.True(ScaffoldForeignKeyShape.HasExactOrderedUniqueIndex(
            indexes,
            "Order",
            new[] { "ExternalNo", "TenantId" }));
    }

    [Theory]
    [InlineData("length(Name) VIRTUAL", "length(Name)", false)]
    [InlineData("length(Name) STORED", "length(Name)", true)]
    [InlineData("([Total]+[Tax]) PERSISTED", "[Total]+[Tax]", true)]
    [InlineData("GENERATED ALWAYS AS (Price * Quantity) STORED", "Price * Quantity", true)]
    [InlineData("GENERATED ALWAYS AS (Price * Quantity) VIRTUAL", "Price * Quantity", false)]
    [InlineData("GENERATED /* (ignored) */ ALWAYS AS /* (ignored) */ (Price * Quantity) STORED", "Price * Quantity", true)]
    [InlineData("' STORED' VIRTUAL", "' STORED'", false)]
    [InlineData("GENERATED ALWAYS AS (' STORED') VIRTUAL", "' STORED'", false)]
    [InlineData("CONCAT(Name, ' PERSISTED')", "CONCAT(Name, ' PERSISTED')", false)]
    [InlineData("PERSISTED", "PERSISTED", false)]
    public void NormalizeScaffoldComputedSql_StripsStorageTokensAndPreservesStoredFlag(
        string raw,
        string expectedSql,
        bool expectedStored)
    {
        var (sql, stored) = InvokeNormalizeScaffoldComputedSql(raw);

        Assert.Equal(expectedSql, sql);
        Assert.Equal(expectedStored, stored);
    }

    [Theory]
    [InlineData("(length(\"Paren)Name\") + 1)", "length(\"Paren)Name\") + 1")]
    [InlineData("([Paren)Name] + 1) PERSISTED", "[Paren)Name] + 1")]
    [InlineData("(length(`Paren)Name`) + 1) STORED", "length(`Paren)Name`) + 1")]
    public void NormalizeScaffoldComputedSql_StripsOuterParenthesesAroundQuotedIdentifiers(
        string raw,
        string expectedSql)
    {
        var (sql, _) = InvokeNormalizeScaffoldComputedSql(raw);

        Assert.Equal(expectedSql, sql);
    }

    [Fact]
    public void DynamicExtractSqliteGeneratedColumns_UsesSharedQuoteAwareParser()
    {
        var columns = InvokeDynamicExtractSqliteGeneratedColumns("""
            CREATE TABLE "Metrics" (
                "Note" TEXT DEFAULT 'GENERATED ALWAYS AS (ignored) STORED',
                "Total" INTEGER GENERATED ALWAYS AS (([Quantity] * [Price])) PERSISTED,
                "Virtual" TEXT GENERATED ALWAYS AS ('PERSISTED') VIRTUAL,
                "Commented" INTEGER GENERATED /* (ignored) */ ALWAYS AS /* (ignored) */ ([Quantity] + 1) STORED
            )
            """);

        Assert.False(columns.ContainsKey("Note"));
        Assert.Equal("[Quantity] * [Price]", columns["Total"].Sql);
        Assert.True(columns["Total"].Stored);
        Assert.Equal("'PERSISTED'", columns["Virtual"].Sql);
        Assert.False(columns["Virtual"].Stored);
        Assert.Equal("[Quantity] + 1", columns["Commented"].Sql);
        Assert.True(columns["Commented"].Stored);
    }

    [Fact]
    public void CreateIndexMetadataParser_IgnoresSqlCommentsWhenLocatingClauses()
    {
        const string sql = """
            CREATE /* ON fake.Table (broken) */ UNIQUE INDEX "IX_Documents_Commented"
            ON public."Documents"
            USING btree
            (lower("Name") /* ) WHERE "Commented" IS NULL */)
            INCLUDE ("Score", /* ) , "Ignored" */ "Rank")
            /* WHERE "Ignored" = 1 */
            WHERE "Name" IS NOT NULL;
            """;

        Assert.True(ScaffoldSqlMetadataParser.IsCreateIndexUnique(sql));
        Assert.Equal(
            "lower(\"Name\") /* ) WHERE \"Commented\" IS NULL */",
            ScaffoldSqlMetadataParser.ExtractCreateIndexExpressionSql(sql));
        Assert.Equal(
            new[] { "Score", "Rank" },
            ScaffoldSqlMetadataParser.ExtractCreateIndexIncludedColumnNames(sql));
        Assert.Equal(
            "\"Name\" IS NOT NULL",
            ScaffoldSqlMetadataParser.ExtractCreateIndexWhereClause(sql));
    }

    [Fact]
    public void SqliteCheckConstraintParser_IgnoresSqlCommentsAndStringLiterals()
    {
        const string sql = """
            CREATE TABLE "Orders" (
                "Id" INTEGER PRIMARY KEY,
                "Note" TEXT DEFAULT 'CHECK (ignored string literal)',
                "Commented" INTEGER /* CHECK (ignored block comment) */,
                CONSTRAINT "CK_Orders_Amount" CHECK ("Amount" > 0 /* ) ignored comment */),
                "Status" TEXT CONSTRAINT [CK_Orders_Status] CHECK ("Status" IN ('new', 'done')),
                "Code" TEXT CONSTRAINT [UQ_Orders_Code] UNIQUE CHECK (length("Code") > 0),
                "NamedCode" TEXT CONSTRAINT [UQ_Orders_NamedCode] UNIQUE CONSTRAINT [CK_Orders_NamedCode] CHECK (length("NamedCode") > 0),
                -- CHECK (ignored line comment)
                "Flag" INTEGER CHECK /* CHECK (ignored trivia comment) */ ("Flag" IN (0, 1))
            );
            """;

        var checks = ScaffoldSqliteDdlParser.ExtractCheckConstraints("Orders", sql);

        Assert.Equal(5, checks.Count);
        Assert.Equal(("CK_Orders_Amount", "\"Amount\" > 0 /* ) ignored comment */"), checks[0]);
        Assert.Equal(("CK_Orders_Status", "\"Status\" IN ('new', 'done')"), checks[1]);
        Assert.Equal(("CK_Orders_1", "length(\"Code\") > 0"), checks[2]);
        Assert.Equal(("CK_Orders_NamedCode", "length(\"NamedCode\") > 0"), checks[3]);
        Assert.Equal(("CK_Orders_2", "\"Flag\" IN (0, 1)"), checks[4]);
    }

    [Fact]
    public void SqlitePrimaryKeyConstraintParser_ExtractsOnlyNamedPrimaryKeyConstraints()
    {
        const string tableConstraintSql = """
            CREATE TABLE "Orders" (
                "TenantId" INTEGER NOT NULL,
                "OrderId" INTEGER NOT NULL,
                "Note" TEXT DEFAULT 'PRIMARY KEY ignored',
                CONSTRAINT "PK_Orders" PRIMARY /* ignored */ KEY ("TenantId", "OrderId")
            );
            """;
        const string columnConstraintSql = """
            CREATE TABLE "ColumnOrders" (
                "Id" INTEGER CONSTRAINT [PK_ColumnOrders] PRIMARY KEY,
                "Note" TEXT CONSTRAINT [CK_ColumnOrders_Note] CHECK (length("Note") > 0)
            );
            """;
        const string unnamedSql = """
            CREATE TABLE "UnnamedOrders" (
                "Id" INTEGER CONSTRAINT [CK_UnnamedOrders_Id] CHECK ("Id" > 0) PRIMARY KEY,
                "Note" TEXT
            );
            """;

        Assert.Equal("PK_Orders", ScaffoldSqliteDdlParser.ExtractPrimaryKeyConstraintName(tableConstraintSql));
        Assert.Equal("PK_ColumnOrders", ScaffoldSqliteDdlParser.ExtractPrimaryKeyConstraintName(columnConstraintSql));
        Assert.Null(ScaffoldSqliteDdlParser.ExtractPrimaryKeyConstraintName(unnamedSql));
    }

    [Fact]
    public void SqliteForeignKeyConstraintParser_ExtractsNamedTableAndColumnConstraints()
    {
        const string sql = """
            CREATE TABLE "Orders" (
                "Id" INTEGER PRIMARY KEY,
                "CustomerId" INTEGER CONSTRAINT [FK_Orders_Customer] REFERENCES "Customer"("Id"),
                "TenantId" INTEGER NOT NULL,
                "OrderNo" INTEGER NOT NULL,
                "Note" TEXT DEFAULT 'FOREIGN KEY ignored',
                CONSTRAINT "FK_Orders_Tenant" FOREIGN /* ignored */ KEY ("TenantId", "OrderNo") REFERENCES "TenantOrder"("TenantId", "OrderNo"),
                FOREIGN KEY ("Id") REFERENCES "Audit"("OrderId")
            );
            """;

        var names = ScaffoldSqliteDdlParser.ExtractForeignKeyConstraintNamesByColumns(sql);

        Assert.Equal("FK_Orders_Customer", names[ScaffoldSqliteDdlParser.BuildForeignKeyColumnKey(new[] { "CustomerId" })]);
        Assert.Equal("FK_Orders_Tenant", names[ScaffoldSqliteDdlParser.BuildForeignKeyColumnKey(new[] { "TenantId", "OrderNo" })]);
        Assert.False(names.ContainsKey(ScaffoldSqliteDdlParser.BuildForeignKeyColumnKey(new[] { "Id" })));
    }

    [Fact]
    public void SqliteUniqueConstraintParser_ExtractsNamedTableAndColumnConstraints()
    {
        const string sql = """
            CREATE TABLE "UniqueConstraintWidget" (
                "Id" INTEGER PRIMARY KEY,
                "Code" TEXT NOT NULL CONSTRAINT [UQ_UniqueConstraintWidget_Code] UNIQUE,
                "TenantId" INTEGER NOT NULL,
                "ExternalNo" TEXT NOT NULL,
                "Note" TEXT DEFAULT 'UNIQUE ignored',
                CONSTRAINT "UQ_UniqueConstraintWidget_Tenant_External" UNIQUE /* ignored */ ("TenantId", "ExternalNo"),
                UNIQUE ("Id")
            );
            """;

        var names = ScaffoldSqliteDdlParser.ExtractUniqueConstraintNamesByColumns(sql);

        Assert.Equal("UQ_UniqueConstraintWidget_Code", names[ScaffoldSqliteDdlParser.BuildColumnListKey(new[] { "Code" })]);
        Assert.Equal(
            "UQ_UniqueConstraintWidget_Tenant_External",
            names[ScaffoldSqliteDdlParser.BuildColumnListKey(new[] { "TenantId", "ExternalNo" })]);
        Assert.False(names.ContainsKey(ScaffoldSqliteDdlParser.BuildColumnListKey(new[] { "Id" })));
    }

    [Fact]
    public void SqliteCollationParser_IgnoresSqlCommentsAndStringLiterals()
    {
        const string sql = """
            CREATE TABLE "Documents" (
                "Id" INTEGER PRIMARY KEY,
                "Name" TEXT DEFAULT 'COLLATE ignored' COLLATE NOCASE,
                "Commented" TEXT /* COLLATE ignored */,
                "Tagged" TEXT COLLATE /* COLLATE ignored trivia */ "custom-name",
                "Flag" TEXT COLLATE [BINARY]
            );
            """;

        var collations = ScaffoldSqliteDdlParser.ExtractColumnCollations(sql);

        Assert.Equal(3, collations.Count);
        Assert.Equal("NOCASE", collations["Name"]);
        Assert.Equal("custom-name", collations["Tagged"]);
        Assert.Equal("BINARY", collations["Flag"]);
        Assert.False(collations.ContainsKey("Commented"));
    }

    [Theory]
    [InlineData("CHECK ((length(\"Paren)Name\") > 0))", "length(\"Paren)Name\") > 0")]
    [InlineData("CHECK (([Paren)Name] > 0))", "[Paren)Name] > 0")]
    [InlineData("CHECK /* (ignored) */ ((length(Name) > 0))", "length(Name) > 0")]
    [InlineData("CHECK (CHECKSUM([Name]) > 0)", "CHECKSUM([Name]) > 0")]
    [InlineData("CHECKSUM([Name]) > 0", "CHECKSUM([Name]) > 0")]
    public void NormalizeScaffoldCheckSql_StripsOuterParenthesesAroundQuotedIdentifiers(
        string raw,
        string expectedSql)
    {
        var sql = InvokeNormalizeScaffoldCheckSql(raw);

        Assert.Equal(expectedSql, sql);
    }

    [Theory]
    [InlineData("'active'::text")]
    [InlineData("'draft'::character varying")]
    [InlineData("'{}'::jsonb")]
    [InlineData("0xDEADBEEF")]
    [InlineData("X'DEADBEEF'")]
    [InlineData("x''")]
    [InlineData("'\\xDEADBEEF'::bytea")]
    [InlineData("'00000000-0000-0000-0000-000000000000'::uuid")]
    [InlineData("42::integer")]
    [InlineData("3.14::numeric(10, 2)")]
    [InlineData("true::boolean")]
    [InlineData("now()::timestamp without time zone")]
    [InlineData("CURRENT_TIMESTAMP::timestamp with time zone")]
    [InlineData("now() AT TIME ZONE 'utc'")]
    [InlineData("CURRENT_TIMESTAMP(6) AT TIME ZONE 'utc'::text")]
    [InlineData("timezone('utc'::text, now())")]
    [InlineData("lower('NEW')")]
    [InlineData("UPPER(N'pending')")]
    [InlineData("lower('can''t ship')")]
    [InlineData("lower('NEW'::text)")]
    [InlineData("UPPER('pending'::character varying)")]
    [InlineData("lower(_utf8mb4'NEW')")]
    public void NormalizeDefaultSql_StaticAndDynamic_AcceptSafePostgresCasts(string raw)
    {
        var staticResult = InvokeTryNormalizeScaffoldDefaultSql(raw);
        var dynamicResult = InvokeTryNormalizeDynamicDefaultSql(raw);

        Assert.True(staticResult.Normalized);
        Assert.Equal(raw, staticResult.Sql);
        Assert.True(dynamicResult.Normalized);
        Assert.Equal(raw, dynamicResult.Sql);
    }

    [Theory]
    [InlineData("'active'::text; DROP TABLE Users")]
    [InlineData("'active'::text -- comment")]
    [InlineData("0::integer /* comment */")]
    [InlineData("0xDEADBEEF; DROP TABLE Users")]
    [InlineData("X'DEADBEEF'; DROP TABLE Users")]
    [InlineData("X'DEADZ0'")]
    [InlineData("X'DEA'")]
    [InlineData("now()::timestamp without time zone; DELETE FROM Users")]
    [InlineData("now() AT TIME ZONE current_user")]
    [InlineData("timezone('utc', unsafe())")]
    [InlineData("timezone('utc', now()); DROP TABLE Users")]
    [InlineData("'active'::\"quoted\"")]
    [InlineData("lower(Status)")]
    [InlineData("upper(current_user)")]
    [InlineData("lower('active'::\"quoted\")")]
    [InlineData("lower('active'); DROP TABLE Users")]
    [InlineData("lower('active'::text); DROP TABLE Users")]
    [InlineData("lower(_utf8mb4 Status)")]
    [InlineData("CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP")]
    [InlineData("CURRENT_TIMESTAMP DEFAULT_GENERATED on update CURRENT_TIMESTAMP")]
    [InlineData("replace('NEW','N','n')")]
    public void NormalizeDefaultSql_StaticAndDynamic_RejectUnsafePostgresCasts(string raw)
    {
        Assert.False(InvokeTryNormalizeScaffoldDefaultSql(raw).Normalized);
        Assert.False(InvokeTryNormalizeDynamicDefaultSql(raw).Normalized);
    }

    [Theory]
    [InlineData("IDENTITY(1000,25)", true, 1000L, 25L)]
    [InlineData(" IDENTITY ( -5 , 2 ) NOT FOR REPLICATION", true, -5L, 2L)]
    [InlineData("GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1)", false, 0L, 0L)]
    [InlineData("sequence(1000,25)", false, 0L, 0L)]
    [InlineData("IDENTITY_COMMENT(1000,25)", false, 0L, 0L)]
    public void TryParseIdentityOptions_OnlyAcceptsSqlServerIdentityMetadata(
        string detail,
        bool expectedParsed,
        long expectedSeed,
        long expectedIncrement)
    {
        var (parsed, seed, increment) = InvokeTryParseIdentityOptions(detail);

        Assert.Equal(expectedParsed, parsed);
        Assert.Equal(expectedSeed, seed);
        Assert.Equal(expectedIncrement, increment);
    }
}
