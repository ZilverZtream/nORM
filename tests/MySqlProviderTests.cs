using System.Text;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class MySqlProviderTests
{
    [Fact]
    public void ApplyPaging_with_only_offset_adds_max_limit()
    {
        var provider = new MySqlProvider();
        var sb = new StringBuilder("SELECT * FROM `Product`");
        var offsetParam = provider.ParamPrefix + "p0";
        provider.ApplyPaging(sb, null, 20, null, offsetParam);
        Assert.Equal($"SELECT * FROM `Product` LIMIT {offsetParam}, 18446744073709551615", sb.ToString());
    }
}
