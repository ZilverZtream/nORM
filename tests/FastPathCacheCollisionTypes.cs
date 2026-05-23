namespace Alpha
{
    [Xunit.Trait("Category", "Fast")]
    public class Collision
    {
        public int Id { get; set; }
        public string Value { get; set; } = "";
    }
}

namespace Beta
{
    [Xunit.Trait("Category", "Fast")]
    public class Collision
    {
        public int Id { get; set; }
        public int Score { get; set; }
    }
}
