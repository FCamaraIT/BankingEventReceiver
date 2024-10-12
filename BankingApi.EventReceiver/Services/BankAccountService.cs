
using BankingApi.EventReceiver.Errors;
using BankingApi.EventReceiver.Repositories;
using Microsoft.Extensions.Logging;

namespace BankingApi.EventReceiver.Services
{
    public class BankAccountService : IBankAccountService
    {
        private readonly IBankAccountRepository _bankAccountRepository;
        private readonly ILogger<BankAccountService> _logger;

        public BankAccountService(IBankAccountRepository bankAccountRepository, ILogger<BankAccountService> logger)
        {
            _bankAccountRepository = bankAccountRepository;
            _logger = logger;
        }

        public async Task CreditToAccount(Guid id, decimal amount, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Bank Account Service | Credting amount to account: {ID}", id);
            var account = await _bankAccountRepository.GetAccount(id);

            if (account == null)
            {
                _logger.LogError("Bank Account Service | Account with id {ID} not found", id);
                throw new NonTransientException($"Bank account with Id {id} not found");
            }

            account.Credit(amount);
            await _bankAccountRepository.UpdateAccount(account, cancellationToken);
        }

        public async Task DebitFromAccount(Guid id, decimal amount, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Bank Account Service | Debiting amount from account: {ID}", id);
            var account = await _bankAccountRepository.GetAccount(id);

            if (account == null)
            {
                _logger.LogError("Bank Account Service | Account with id {ID} not found", id);
                throw new NonTransientException($"Bank account with Id {id} not found");
            }

            account.Debit(amount);
            await _bankAccountRepository.UpdateAccount(account, cancellationToken);
        }
    }
}
