#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    // Relationship scaffold integration tests.

    [Fact]
    public async Task ScaffoldAsync_WithSingleColumnForeignKey_GeneratesNavigationsAndModelConfig()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL
            );
            CREATE TABLE Book (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Author_Id INTEGER NOT NULL,
                Title TEXT NOT NULL,
                FOREIGN KEY (Author_Id) REFERENCES Author(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "BookStoreContext");

            var authorCode = File.ReadAllText(Path.Combine(dir, "Author.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "Book.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "BookStoreContext.cs"));

            Assert.Contains("public List<Book> Books { get; set; } = new();", authorCode);
            Assert.Contains("[ForeignKey(nameof(AuthorId))]", bookCode);
            Assert.Contains("public Author Author { get; set; } = default!;", bookCode);
            Assert.Contains(".HasMany(p => p.Books)", contextCode);
            Assert.Contains(".WithOne(d => d.Author)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.AuthorId, p => p.Id, cascadeDelete: false);", contextCode);
            Assert.Contains("configure?.Invoke(mb);", contextCode);
            Assert.Contains("var configuredOptions = options?.Clone() ?? new DbContextOptions();", contextCode);
            Assert.DoesNotContain("options.OnModelCreating =", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNoRelationships_EmitsScalarForeignKeysWithoutNavigations()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL
            );
            CREATE TABLE Book (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Author_Id INTEGER NOT NULL,
                Title TEXT NOT NULL,
                FOREIGN KEY (Author_Id) REFERENCES Author(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_no_relationships_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "NoRelationshipsCtx",
                new ScaffoldOptions { NoRelationships = true });

            var authorCode = File.ReadAllText(Path.Combine(dir, "Author.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "Book.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "NoRelationshipsCtx.cs"));

            Assert.DoesNotContain("List<Book>", authorCode, StringComparison.Ordinal);
            Assert.Contains("AuthorId", bookCode, StringComparison.Ordinal);
            Assert.DoesNotContain("[ForeignKey(", bookCode, StringComparison.Ordinal);
            Assert.DoesNotContain("public Author Author", bookCode, StringComparison.Ordinal);
            Assert.DoesNotContain(".HasMany(", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain(".HasOne(", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain(".WithOne(", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain(".HasForeignKey(", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithUniqueForeignKey_GeneratesOneToOneNavigationsAndModelConfig()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE User (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            CREATE TABLE UserProfile (
                Id INTEGER PRIMARY KEY,
                UserId INTEGER NOT NULL UNIQUE,
                DisplayName TEXT NOT NULL,
                CONSTRAINT FK_UserProfile_User FOREIGN KEY (UserId) REFERENCES User(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_one_to_one_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "OneToOneCtx");

            var userCode = File.ReadAllText(Path.Combine(dir, "User.cs"));
            var profileCode = File.ReadAllText(Path.Combine(dir, "UserProfile.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "OneToOneCtx.cs"));

            Assert.Contains("public UserProfile? UserProfile { get; set; }", userCode);
            Assert.DoesNotContain("List<UserProfile>", userCode, StringComparison.Ordinal);
            Assert.Contains("[ForeignKey(nameof(UserId))]", profileCode);
            Assert.Contains("public User User { get; set; } = default!;", profileCode);
            Assert.Contains(".HasOne(p => p.UserProfile)", contextCode);
            Assert.Contains(".WithOne(d => d.User)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.UserId, p => p.Id, \"FK_UserProfile_User\", false);", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNullableUniqueForeignKey_GeneratesOptionalOneToOneNavigationsAndModelConfig()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Person (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL
            );
            CREATE TABLE PersonAvatar (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                PersonId INTEGER NULL UNIQUE,
                Url TEXT NOT NULL,
                CONSTRAINT FK_PersonAvatar_Person FOREIGN KEY (PersonId) REFERENCES Person(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_optional_one_to_one_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "OptionalOneToOneCtx");

            var personCode = File.ReadAllText(Path.Combine(dir, "Person.cs"));
            var avatarCode = File.ReadAllText(Path.Combine(dir, "PersonAvatar.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "OptionalOneToOneCtx.cs"));

            Assert.Contains("public PersonAvatar? PersonAvatar { get; set; }", personCode);
            Assert.DoesNotContain("List<PersonAvatar>", personCode, StringComparison.Ordinal);
            Assert.Matches(@"public (int|long)\? PersonId \{ get; set; \}", avatarCode);
            Assert.Contains("[ForeignKey(nameof(PersonId))]", avatarCode);
            Assert.Contains("public Person? Person { get; set; }", avatarCode);
            Assert.DoesNotContain("public Person Person { get; set; } = default!;", avatarCode, StringComparison.Ordinal);
            Assert.Contains(".HasOne(p => p.PersonAvatar)", contextCode);
            Assert.Contains(".WithOne(d => d.Person)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.PersonId, p => p.Id, \"FK_PersonAvatar_Person\", false);", contextCode);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositeUniqueForeignKey_GeneratesOneToOneNavigationsAndModelConfig()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Account (
                Id INTEGER PRIMARY KEY,
                TenantId INTEGER NOT NULL,
                AccountNo INTEGER NOT NULL,
                Name TEXT NOT NULL,
                UNIQUE (TenantId, AccountNo)
            );
            CREATE TABLE AccountProfile (
                Id INTEGER PRIMARY KEY,
                TenantId INTEGER NOT NULL,
                AccountNo INTEGER NOT NULL,
                DisplayName TEXT NOT NULL,
                UNIQUE (TenantId, AccountNo),
                CONSTRAINT FK_AccountProfile_Account FOREIGN KEY (TenantId, AccountNo) REFERENCES Account(TenantId, AccountNo)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_composite_one_to_one_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositeOneToOneCtx");

            var accountCode = File.ReadAllText(Path.Combine(dir, "Account.cs"));
            var profileCode = File.ReadAllText(Path.Combine(dir, "AccountProfile.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "CompositeOneToOneCtx.cs"));

            Assert.Contains("public AccountProfile? AccountProfile { get; set; }", accountCode);
            Assert.DoesNotContain("List<AccountProfile>", accountCode, StringComparison.Ordinal);
            Assert.DoesNotContain("[ForeignKey(", profileCode, StringComparison.Ordinal);
            Assert.Contains("public Account Account { get; set; } = default!;", profileCode);
            Assert.Contains(".HasOne(p => p.AccountProfile)", contextCode);
            Assert.Contains(".WithOne(d => d.Account)", contextCode);
            Assert.Contains(".HasForeignKey(d => new { d.TenantId, d.AccountNo }, p => new { p.TenantId, p.AccountNo }, \"FK_AccountProfile_Account\", false);", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithNullableCompositeUniqueForeignKey_GeneratesOptionalOneToOneNavigationsAndModelConfig()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Account (
                Id INTEGER PRIMARY KEY,
                TenantId INTEGER NOT NULL,
                AccountNo INTEGER NOT NULL,
                Name TEXT NOT NULL,
                UNIQUE (TenantId, AccountNo)
            );
            CREATE TABLE AccountProfile (
                Id INTEGER PRIMARY KEY,
                TenantId INTEGER NOT NULL,
                AccountNo INTEGER NULL,
                DisplayName TEXT NOT NULL,
                UNIQUE (TenantId, AccountNo),
                CONSTRAINT FK_AccountProfile_Account FOREIGN KEY (TenantId, AccountNo) REFERENCES Account(TenantId, AccountNo)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_optional_composite_one_to_one_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "OptionalCompositeOneToOneCtx");

            var accountCode = File.ReadAllText(Path.Combine(dir, "Account.cs"));
            var profileCode = File.ReadAllText(Path.Combine(dir, "AccountProfile.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "OptionalCompositeOneToOneCtx.cs"));

            Assert.Contains("public AccountProfile? AccountProfile { get; set; }", accountCode);
            Assert.DoesNotContain("List<AccountProfile>", accountCode, StringComparison.Ordinal);
            Assert.Matches(@"public (int|long) TenantId \{ get; set; \}", profileCode);
            Assert.Matches(@"public (int|long)\? AccountNo \{ get; set; \}", profileCode);
            Assert.DoesNotContain("[ForeignKey(", profileCode, StringComparison.Ordinal);
            Assert.Contains("public Account? Account { get; set; }", profileCode);
            Assert.DoesNotContain("public Account Account { get; set; } = default!;", profileCode, StringComparison.Ordinal);
            Assert.Contains(".HasOne(p => p.AccountProfile)", contextCode);
            Assert.Contains(".WithOne(d => d.Account)", contextCode);
            Assert.Contains(".HasForeignKey(d => new { d.TenantId, d.AccountNo }, p => new { p.TenantId, p.AccountNo }, \"FK_AccountProfile_Account\", false);", contextCode);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSharedPrimaryKeyForeignKey_GeneratesOneToOneNavigationsAndModelConfig()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Customer (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL
            );
            CREATE TABLE CustomerProfile (
                Id INTEGER NOT NULL PRIMARY KEY,
                DisplayName TEXT NOT NULL,
                CONSTRAINT FK_CustomerProfile_Customer FOREIGN KEY (Id) REFERENCES Customer(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SharedPkOneToOneCtx");

            var customerCode = File.ReadAllText(Path.Combine(dir, "Customer.cs"));
            var profileCode = File.ReadAllText(Path.Combine(dir, "CustomerProfile.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "SharedPkOneToOneCtx.cs"));

            Assert.Contains("public CustomerProfile? CustomerProfile { get; set; }", customerCode);
            Assert.DoesNotContain("List<CustomerProfile>", customerCode, StringComparison.Ordinal);
            Assert.Contains("[ForeignKey(nameof(Id))]", profileCode);
            Assert.Contains("public Customer Customer { get; set; } = default!;", profileCode);
            Assert.Contains(".HasOne(p => p.CustomerProfile)", contextCode);
            Assert.Contains(".WithOne(d => d.Customer)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.Id, p => p.Id, \"FK_CustomerProfile_Customer\", false);", contextCode);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithMultipleForeignKeysToSamePrincipal_UsesRoleBasedNavigationNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Address (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Line1 TEXT NOT NULL
            );
            CREATE TABLE Shipment (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                BillingAddressId INTEGER NOT NULL,
                ShippingAddressId INTEGER NOT NULL,
                CONSTRAINT FK_Shipment_BillingAddress FOREIGN KEY (BillingAddressId) REFERENCES Address(Id),
                CONSTRAINT FK_Shipment_ShippingAddress FOREIGN KEY (ShippingAddressId) REFERENCES Address(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ShipmentContext");

            var addressCode = File.ReadAllText(Path.Combine(dir, "Address.cs"));
            var shipmentCode = File.ReadAllText(Path.Combine(dir, "Shipment.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "ShipmentContext.cs"));

            Assert.Contains("public List<Shipment> ShipmentsByBillingAddressId { get; set; } = new();", addressCode);
            Assert.Contains("public List<Shipment> ShipmentsByShippingAddressId { get; set; } = new();", addressCode);
            Assert.Contains("[ForeignKey(nameof(BillingAddressId))]", shipmentCode);
            Assert.Contains("public Address BillingAddress { get; set; } = default!;", shipmentCode);
            Assert.Contains("[ForeignKey(nameof(ShippingAddressId))]", shipmentCode);
            Assert.Contains("public Address ShippingAddress { get; set; } = default!;", shipmentCode);
            Assert.DoesNotContain("public Address? Address { get; set; }", shipmentCode);
            Assert.Contains(".HasMany(p => p.ShipmentsByBillingAddressId)", contextCode);
            Assert.Contains(".WithOne(d => d.BillingAddress)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.BillingAddressId, p => p.Id, \"FK_Shipment_BillingAddress\", false);", contextCode);
            Assert.Contains(".HasMany(p => p.ShipmentsByShippingAddressId)", contextCode);
            Assert.Contains(".WithOne(d => d.ShippingAddress)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.ShippingAddressId, p => p.Id, \"FK_Shipment_ShippingAddress\", false);", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithMultipleCompositeForeignKeysToSamePrincipal_UsesDistinguishingRoleNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Account (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                TenantId INTEGER NOT NULL,
                AccountNo INTEGER NOT NULL,
                Name TEXT NOT NULL,
                UNIQUE (TenantId, AccountNo)
            );
            CREATE TABLE AccountTransfer (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                TenantId INTEGER NOT NULL,
                PrimaryAccountNo INTEGER NOT NULL,
                BackupAccountNo INTEGER NOT NULL,
                Amount INTEGER NOT NULL,
                CONSTRAINT FK_Transfer_PrimaryAccount FOREIGN KEY (TenantId, PrimaryAccountNo) REFERENCES Account(TenantId, AccountNo),
                CONSTRAINT FK_Transfer_BackupAccount FOREIGN KEY (TenantId, BackupAccountNo) REFERENCES Account(TenantId, AccountNo)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositeRoleContext");

            var accountCode = File.ReadAllText(Path.Combine(dir, "Account.cs"));
            var transferCode = File.ReadAllText(Path.Combine(dir, "AccountTransfer.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "CompositeRoleContext.cs"));

            Assert.Contains("public List<AccountTransfer> AccountTransfersByBackupAccountNo { get; set; } = new();", accountCode);
            Assert.Contains("public List<AccountTransfer> AccountTransfersByPrimaryAccountNo { get; set; } = new();", accountCode);
            Assert.Contains("public Account BackupAccount { get; set; } = default!;", transferCode);
            Assert.Contains("public Account PrimaryAccount { get; set; } = default!;", transferCode);
            Assert.DoesNotContain("public Account Tenant", transferCode, StringComparison.Ordinal);
            Assert.DoesNotContain("AccountTransfersByTenantId", accountCode, StringComparison.Ordinal);
            Assert.Contains(".HasMany(p => p.AccountTransfersByBackupAccountNo)", contextCode);
            Assert.Contains(".WithOne(d => d.BackupAccount)", contextCode);
            Assert.Contains(".HasForeignKey(d => new { d.TenantId, d.BackupAccountNo }, p => new { p.TenantId, p.AccountNo }, \"FK_Transfer_BackupAccount\", false);", contextCode);
            Assert.Contains(".HasMany(p => p.AccountTransfersByPrimaryAccountNo)", contextCode);
            Assert.Contains(".WithOne(d => d.PrimaryAccount)", contextCode);
            Assert.Contains(".HasForeignKey(d => new { d.TenantId, d.PrimaryAccountNo }, p => new { p.TenantId, p.AccountNo }, \"FK_Transfer_PrimaryAccount\", false);", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithMultipleUniqueForeignKeysToSamePrincipal_UsesRoleBasedOneToOneNavigationNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Account (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL
            );
            CREATE TABLE AccountProfile (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                PrimaryAccountId INTEGER NOT NULL UNIQUE,
                BackupAccountId INTEGER NOT NULL UNIQUE,
                DisplayName TEXT NOT NULL,
                CONSTRAINT FK_Profile_PrimaryAccount FOREIGN KEY (PrimaryAccountId) REFERENCES Account(Id),
                CONSTRAINT FK_Profile_BackupAccount FOREIGN KEY (BackupAccountId) REFERENCES Account(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "UniqueRoleOneToOneContext");

            var accountCode = File.ReadAllText(Path.Combine(dir, "Account.cs"));
            var profileCode = File.ReadAllText(Path.Combine(dir, "AccountProfile.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "UniqueRoleOneToOneContext.cs"));

            Assert.Contains("public AccountProfile? AccountProfileByBackupAccountId { get; set; }", accountCode);
            Assert.Contains("public AccountProfile? AccountProfileByPrimaryAccountId { get; set; }", accountCode);
            Assert.DoesNotContain("List<AccountProfile>", accountCode, StringComparison.Ordinal);
            Assert.Contains("[ForeignKey(nameof(BackupAccountId))]", profileCode);
            Assert.Contains("public Account BackupAccount { get; set; } = default!;", profileCode);
            Assert.Contains("[ForeignKey(nameof(PrimaryAccountId))]", profileCode);
            Assert.Contains("public Account PrimaryAccount { get; set; } = default!;", profileCode);
            Assert.DoesNotContain("public Account Account { get; set; }", profileCode, StringComparison.Ordinal);
            Assert.Contains(".HasOne(p => p.AccountProfileByBackupAccountId)", contextCode);
            Assert.Contains(".WithOne(d => d.BackupAccount)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.BackupAccountId, p => p.Id, \"FK_Profile_BackupAccount\", false);", contextCode);
            Assert.Contains(".HasOne(p => p.AccountProfileByPrimaryAccountId)", contextCode);
            Assert.Contains(".WithOne(d => d.PrimaryAccount)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.PrimaryAccountId, p => p.Id, \"FK_Profile_PrimaryAccount\", false);", contextCode);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithSelfReferencingForeignKey_UsesRoleBasedNavigationNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Person (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                ParentId INTEGER NULL,
                Name TEXT NOT NULL,
                CONSTRAINT FK_Person_Parent FOREIGN KEY (ParentId) REFERENCES Person(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "PersonContext");

            var personCode = File.ReadAllText(Path.Combine(dir, "Person.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "PersonContext.cs"));

            Assert.Contains("[ForeignKey(nameof(ParentId))]", personCode);
            Assert.Contains("public Person? Parent { get; set; }", personCode);
            Assert.Contains("public List<Person> PeopleByParentId { get; set; } = new();", personCode);
            Assert.DoesNotContain("public Person? Person { get; set; }", personCode);
            Assert.Contains(".HasMany(p => p.PeopleByParentId)", contextCode);
            Assert.Contains(".WithOne(d => d.Parent)", contextCode);
            Assert.Contains(".HasForeignKey(d => d.ParentId, p => p.Id, \"FK_Person_Parent\", false);", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WhenNavigationNameCollidesWithScalarProperty_MakesNavigationUnique()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Author (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Books TEXT NOT NULL
            );
            CREATE TABLE Book (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                AuthorId INTEGER NOT NULL,
                Author TEXT NOT NULL,
                CONSTRAINT FK_Book_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CollisionNavCtx");

            var authorCode = File.ReadAllText(Path.Combine(dir, "Author.cs"));
            var bookCode = File.ReadAllText(Path.Combine(dir, "Book.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "CollisionNavCtx.cs"));

            Assert.Contains("public string Books { get; set; } = default!;", authorCode);
            Assert.Contains("public List<Book> BooksByAuthorId { get; set; } = new();", authorCode);
            Assert.Contains("public string Author { get; set; } = default!;", bookCode);
            Assert.Contains("public Author Author2 { get; set; } = default!;", bookCode);
            Assert.Contains(".HasMany(p => p.BooksByAuthorId)", contextCode);
            Assert.Contains(".WithOne(d => d.Author2)", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

}
