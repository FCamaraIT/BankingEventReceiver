namespace BankingApi.EventReceiver;

public class BankOperation
{
    public Guid Id { get; set; }
    public string? MessageType { get; set; }
    public Guid BankAccountId { get; set; }
    public int Amount { get; set; }
}