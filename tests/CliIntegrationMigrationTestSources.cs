using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using nORM.Cli;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

public partial class CliIntegrationTests
{
    private static string ModelProjectXml(string root)
    {
        var projectReference = Path.Combine(root, "src", "nORM.csproj");
        return $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
              </PropertyGroup>
              <ItemGroup>
                <ProjectReference Include="{{projectReference}}" />
              </ItemGroup>
            </Project>
            """;
    }

    private static string ModelProjectWithDependencyXml(string root, string dependencyProject)
    {
        var projectReference = Path.Combine(root, "src", "nORM.csproj");
        return $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
              </PropertyGroup>
              <ItemGroup>
                <ProjectReference Include="{{projectReference}}" />
                <ProjectReference Include="{{dependencyProject}}" />
              </ItemGroup>
            </Project>
            """;
    }

    private static string MultiTargetModelProjectXml(string root)
    {
        var projectReference = Path.Combine(root, "src", "nORM.csproj");
        return $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFrameworks>net8.0;netstandard2.1</TargetFrameworks>
                <Nullable>enable</Nullable>
              </PropertyGroup>
              <ItemGroup Condition="'$(TargetFramework)' == 'net8.0'">
                <ProjectReference Include="{{projectReference}}" />
              </ItemGroup>
            </Project>
            """;
    }

    private static string StartupHostProjectXml(string root)
    {
        var projectReference = Path.Combine(root, "src", "nORM.csproj");
        return $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
              </PropertyGroup>
              <ItemGroup>
                <ProjectReference Include="{{projectReference}}" />
              </ItemGroup>
            </Project>
            """;
    }

    private const string SimpleNet8ProjectXml = """
        <Project Sdk="Microsoft.NET.Sdk">
          <PropertyGroup>
            <TargetFramework>net8.0</TargetFramework>
            <Nullable>enable</Nullable>
          </PropertyGroup>
        </Project>
        """;

    private const string DependencyProjectXml = """
        <Project Sdk="Microsoft.NET.Sdk">
          <PropertyGroup>
            <TargetFramework>net8.0</TargetFramework>
            <Nullable>enable</Nullable>
          </PropertyGroup>
        </Project>
        """;

    private static string RoundTripProjectXml(string root)
    {
        var projectReference = Path.Combine(root, "src", "nORM.csproj");
        return $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <OutputType>Exe</OutputType>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
                <UseAppHost>false</UseAppHost>
              </PropertyGroup>
              <ItemGroup>
                <ProjectReference Include="{{projectReference}}" />
              </ItemGroup>
            </Project>
            """;
    }

    private const string WeirdModelSource = """
        using System.ComponentModel.DataAnnotations;
        using System.ComponentModel.DataAnnotations.Schema;

        [Table("Odd\"Table\\Line\nBreak")]
        public sealed class WeirdEntity
        {
            [Key]
            public int Id { get; set; }

            [Column("Value\"Column\\Line\nBreak")]
            public string Value { get; set; } = "";
        }
        """;

    private const string RenameModelSource = """
        using System.ComponentModel.DataAnnotations;
        using System.ComponentModel.DataAnnotations.Schema;

        [Table("Orders")]
        public sealed class Order
        {
            [Key]
            public int Id { get; set; }

            public decimal? TotalAmount { get; set; }
        }
        """;

    private const string DesignTimeFactoryModelSource = """
        using System.Data.Common;
        using Microsoft.Data.Sqlite;
        using nORM.Configuration;
        using nORM.Core;
        using nORM.Migration;
        using nORM.Providers;

        public sealed class FactoryEntity
        {
            public int Id { get; set; }
            public string Name { get; set; } = "";
        }

        public sealed class FactoryContext(DbConnection connection, DatabaseProvider provider, DbContextOptions? options = null)
            : DbContext(connection, provider, options);

        public sealed class FactoryDesignTimeContext : INormDesignTimeDbContextFactory<FactoryContext>
        {
            public FactoryContext CreateDbContext(string[] args)
            {
                var connection = new SqliteConnection("Data Source=:memory:");
                connection.Open();
                var options = new DbContextOptions
                {
                    OnModelCreating = mb => mb.Entity<FactoryEntity>()
                        .ToTable("FactoryTable")
                        .HasKey(e => e.Id)
                        .Property(e => e.Name)
                        .HasColumnName("display_name")
                };
                return new FactoryContext(connection, new SqliteProvider(), options);
            }
        }
        """;

    private const string EnvironmentAwareDesignTimeFactoryModelSource = """
        using System;
        using System.Data.Common;
        using System.Linq;
        using Microsoft.Data.Sqlite;
        using nORM.Configuration;
        using nORM.Core;
        using nORM.Migration;
        using nORM.Providers;

        public sealed class EnvironmentEntity
        {
            public int Id { get; set; }
            public string Name { get; set; } = "";
        }

        public sealed class EnvironmentContext(DbConnection connection, DatabaseProvider provider, DbContextOptions? options = null)
            : DbContext(connection, provider, options);

        public sealed class EnvironmentDesignTimeContext : INormDesignTimeDbContextFactory<EnvironmentContext>
        {
            public EnvironmentContext CreateDbContext(string[] args)
            {
                var environment = args.SkipWhile(arg => arg != "--environment").Skip(1).FirstOrDefault()
                    ?? Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")
                    ?? Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT")
                    ?? "Missing";

                var connection = new SqliteConnection("Data Source=:memory:");
                connection.Open();
                var options = new DbContextOptions
                {
                    OnModelCreating = mb => mb.Entity<EnvironmentEntity>()
                        .ToTable(environment + "Table")
                        .HasKey(e => e.Id)
                        .Property(e => e.Name)
                        .HasColumnName(environment + "_name")
                };
                return new EnvironmentContext(connection, new SqliteProvider(), options);
            }
        }
        """;

    private const string FailingDesignTimeFactoryModelSource = """
        using System;
        using System.Data.Common;
        using nORM.Core;
        using nORM.Migration;
        using nORM.Providers;

        public sealed class FailingEntity
        {
            public int Id { get; set; }
        }

        public sealed class FailingContext(DbConnection connection, DatabaseProvider provider)
            : DbContext(connection, provider);

        public sealed class FailingDesignTimeContext : INormDesignTimeDbContextFactory<FailingContext>
        {
            public FailingContext CreateDbContext(string[] args)
            {
                throw new InvalidOperationException("Could not open Server=localhost;Database=norm;User ID=sa;Password=FactorySecret123!;Encrypt=True;");
            }
        }
        """;

    private const string NativeProbeDesignTimeFactoryModelSource = """
        using System.Data.Common;
        using System.Runtime.InteropServices;
        using nORM.Core;
        using nORM.Migration;
        using nORM.Providers;

        public sealed class NativeProbeEntity
        {
            public int Id { get; set; }
        }

        public sealed class NativeProbeContext(DbConnection connection, DatabaseProvider provider)
            : DbContext(connection, provider);

        public sealed class NativeProbeDesignTimeContext : INormDesignTimeDbContextFactory<NativeProbeContext>
        {
            public NativeProbeContext CreateDbContext(string[] args)
            {
                NativeProbe.Missing();
                throw new System.InvalidOperationException("Native probe unexpectedly returned.");
            }
        }

        internal static class NativeProbe
        {
            [DllImport("NativeProbe", EntryPoint = "NormMissingNativeEntryPoint")]
            public static extern int Missing();
        }
        """;

    private const string NativeProbeDepsJson = """
        {
          "runtimeTarget": {
            "name": ".NETCoreApp,Version=v8.0/win-x64",
            "signature": ""
          },
          "targets": {
            ".NETCoreApp,Version=v8.0/win-x64": {
              "NativeProbe/1.0.0": {
                "runtimeTargets": {
                  "runtimes/win-x64/native/NativeProbe.dll": {
                    "rid": "win-x64",
                    "assetType": "native"
                  }
                }
              }
            }
          },
          "libraries": {
            "NativeProbe/1.0.0": {
              "type": "package",
              "serviceable": false,
              "sha512": ""
            }
          }
        }
        """;

    private const string DependencyModelNamesSource = """
        namespace DependencyLib;

        public static class ModelNames
        {
            public const string TableName = "DependentTable";
            public const string NameColumn = "dependency_name";
        }
        """;

    private const string DependencyBackedDesignTimeFactoryModelSource = """
        using System.Data.Common;
        using DependencyLib;
        using Microsoft.Data.Sqlite;
        using nORM.Configuration;
        using nORM.Core;
        using nORM.Migration;
        using nORM.Providers;

        public sealed class DependentEntity
        {
            public int Id { get; set; }
            public string Name { get; set; } = "";
        }

        public sealed class DependentContext(DbConnection connection, DatabaseProvider provider, DbContextOptions? options = null)
            : DbContext(connection, provider, options);

        public sealed class DependentDesignTimeContext : INormDesignTimeDbContextFactory<DependentContext>
        {
            public DependentContext CreateDbContext(string[] args)
            {
                var connection = new SqliteConnection("Data Source=:memory:");
                connection.Open();
                var options = new DbContextOptions
                {
                    OnModelCreating = mb => mb.Entity<DependentEntity>()
                        .ToTable(ModelNames.TableName)
                        .HasKey(e => e.Id)
                        .Property(e => e.Name)
                        .HasColumnName(ModelNames.NameColumn)
                };
                return new DependentContext(connection, new SqliteProvider(), options);
            }
        }
        """;

    private const string RoundTripProgramSource = """
        using System;
        using System.Collections.Generic;
        using System.Data;
        using System.Data.Common;
        using System.IO;
        using System.Linq;
        using System.Text;

        var connection = new RecordingConnection();
        var transaction = new RecordingTransaction(connection);
        var migration = new Migration_202605220001_Adversarial();
        migration.Up(connection, transaction);
        migration.Down(connection, transaction);
        File.WriteAllLines(args[0], connection.Commands.Select(sql => Convert.ToBase64String(Encoding.UTF8.GetBytes(sql))));

        internal sealed class RecordingConnection : DbConnection
        {
            public List<string> Commands { get; } = new();
            public override string ConnectionString { get; set; } = "";
            public override string Database => "Test";
            public override string DataSource => "Test";
            public override string ServerVersion => "1";
            public override ConnectionState State => ConnectionState.Open;
            public override void ChangeDatabase(string databaseName) { }
            public override void Close() { }
            public override void Open() { }
            protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => new RecordingTransaction(this);
            protected override DbCommand CreateDbCommand() => new RecordingCommand(this);
        }

        internal sealed class RecordingTransaction(RecordingConnection connection) : DbTransaction
        {
            public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;
            protected override DbConnection DbConnection => connection;
            public override void Commit() { }
            public override void Rollback() { }
        }

        internal sealed class RecordingCommand(RecordingConnection connection) : DbCommand
        {
            private readonly DbParameterCollection _parameters = new RecordingParameterCollection();
            public override string CommandText { get; set; } = "";
            public override int CommandTimeout { get; set; }
            public override CommandType CommandType { get; set; }
            public override bool DesignTimeVisible { get; set; }
            public override UpdateRowSource UpdatedRowSource { get; set; }
            protected override DbConnection DbConnection { get; set; } = connection;
            protected override DbParameterCollection DbParameterCollection => _parameters;
            protected override DbTransaction? DbTransaction { get; set; }
            public override void Cancel() { }
            public override int ExecuteNonQuery() { connection.Commands.Add(CommandText); return 0; }
            public override object? ExecuteScalar() => null;
            public override void Prepare() { }
            protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
            protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();
        }

        internal sealed class RecordingParameterCollection : DbParameterCollection
        {
            private readonly List<object> _items = new();
            public override int Count => _items.Count;
            public override object SyncRoot => this;
            public override int Add(object value) { _items.Add(value); return _items.Count - 1; }
            public override void AddRange(Array values) { foreach (var value in values) Add(value!); }
            public override void Clear() => _items.Clear();
            public override bool Contains(object value) => _items.Contains(value);
            public override bool Contains(string value) => false;
            public override void CopyTo(Array array, int index) => _items.ToArray().CopyTo(array, index);
            public override System.Collections.IEnumerator GetEnumerator() => _items.GetEnumerator();
            public override int IndexOf(object value) => _items.IndexOf(value);
            public override int IndexOf(string parameterName) => -1;
            public override void Insert(int index, object value) => _items.Insert(index, value);
            public override void Remove(object value) => _items.Remove(value);
            public override void RemoveAt(int index) => _items.RemoveAt(index);
            public override void RemoveAt(string parameterName) { }
            protected override DbParameter GetParameter(int index) => (DbParameter)_items[index];
            protected override DbParameter GetParameter(string parameterName) => throw new IndexOutOfRangeException(parameterName);
            protected override void SetParameter(int index, DbParameter value) => _items[index] = value;
            protected override void SetParameter(string parameterName, DbParameter value) => Add(value);
        }
        """;
}
