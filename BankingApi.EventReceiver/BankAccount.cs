namespace BankingApi.EventReceiver
{
    public class BankAccount
    {
        public Guid Id { get; set; }  // Primary Key
        public Guid AccountId { get; set; }
        public decimal Balance { get; set; }
    }
}
