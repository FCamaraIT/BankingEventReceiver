using Microsoft.EntityFrameworkCore;

namespace BankingApi.EventReceiver
{
    public class BankingApiDbContext : DbContext
    {
        public DbSet<BankAccount> BankAccounts { get; set; }

        public BankingApiDbContext(DbContextOptions<BankingApiDbContext> options) : base(options) {}

        public BankingApiDbContext(DbContextOptions options) : base(options) {}

        protected override void OnConfiguring(DbContextOptionsBuilder options)
        {
            if (!options.IsConfigured)
            {
                options.UseInMemoryDatabase("TestDb");
            }
        }
    }
}
