namespace BankingApi.EventReceiver.Services
{
    public interface IBankAccountService
    {
        Task CreditToAccount(Guid id, decimal amount, CancellationToken cancellationToken);

        Task DebitFromAccount(Guid id, decimal amount, CancellationToken cancellationToken);
    }
}
