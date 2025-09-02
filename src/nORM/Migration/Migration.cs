using System.Data.Common;

#nullable enable

namespace nORM.Migration
{
    public abstract class Migration
    {
        protected Migration(long version, string name)
        {
            Version = version;
            Name = name;
        }
        
        public long Version { get; }
        public string Name { get; }
        
        public abstract void Up(DbConnection connection, DbTransaction transaction);
        public abstract void Down(DbConnection connection, DbTransaction transaction);
    }
}