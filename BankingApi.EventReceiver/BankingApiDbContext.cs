using Microsoft.EntityFrameworkCore;

namespace BankingApi.EventReceiver
{
    public class BankingApiDbContext : DbContext
    {
        public DbSet<BankAccount> BankAccounts { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder options)
            => options.UseSqlServer("Data Source=MANO\\MSSQLSERVER01;Initial Catalog=BankingApiTest;Integrated Security=True;TrustServerCertificate=True;");

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            // Ensure that the primary key is correctly configured, if it's not using the default convention
            modelBuilder.Entity<BankAccount>()
                .HasKey(b => b.Id);  // Explicitly define the primary key
        }
    }
}
