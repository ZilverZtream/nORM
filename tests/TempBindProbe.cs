using System;
using System.Globalization;
using System.IO;
using Microsoft.Data.Sqlite;
using Xunit;

namespace nORM.Tests;
[Trait("Category","Fast")]
public class TempBindProbe
{
    [Fact]
    public void Probe()
    {
        var cn=new SqliteConnection("Data Source=:memory:"); cn.Open();
        string Bind(DateTime dt){ using var c=cn.CreateCommand(); c.CommandText="SELECT CAST(@p AS TEXT)"; c.Parameters.AddWithValue("@p",dt); return (string)c.ExecuteScalar()!; }
        var lines=new System.Text.StringBuilder();
        var vals=new[]{
            new DateTime(2020,6,1,0,0,0,500,DateTimeKind.Utc),
            new DateTime(2020,6,1,0,0,0,0,DateTimeKind.Utc),
            new DateTime(2020,6,1,0,0,0,123,DateTimeKind.Utc),
            new DateTime(2020,6,1,0,0,0,500,DateTimeKind.Unspecified),
        };
        foreach(var v in vals) lines.AppendLine($"raw DateTime({v.Millisecond}ms,{v.Kind}) -> [{Bind(v)}]");
        lines.AppendLine($"formatted .fff of 500ms -> [{new DateTime(2020,6,1,0,0,0,500).ToString("yyyy-MM-dd HH:mm:ss.fff",CultureInfo.InvariantCulture)}]");
        // sub-ms tick
        lines.AppendLine($"raw DateTime+5000ticks -> [{Bind(new DateTime(2020,6,1,0,0,0,500,DateTimeKind.Utc).AddTicks(5000))}]");
        File.WriteAllText(@"C:\Users\Dennis\AppData\Local\Temp\claude\C--Users-Dennis-source-repos-nORM\5eaf9109-97ca-4664-8f19-f7528971ea31\scratchpad\bindprobe.txt", lines.ToString());
    }
}
