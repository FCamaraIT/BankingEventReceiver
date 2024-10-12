using BankingApi.EventReceiver.Models;

namespace BankingApi.EventReceiver.Repositories
{
    public interface IBankAccountRepository
    {
        Task<BankAccount?> GetAccount(Guid id);

        Task UpdateAccount(BankAccount account, CancellationToken cancellationToken);
    }
}
