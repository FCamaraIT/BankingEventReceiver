namespace BankingApi.EventReceiver.Models;

public class BankAccount
{
    public Guid Id { get; set; }
    public decimal Balance { get; set; }

    public BankAccount Credit(decimal amount)
    {
        Balance += amount;
        return this;
    }

    public BankAccount Debit(decimal amount)
    {
        Balance -= amount;
        return this;
    }
}
