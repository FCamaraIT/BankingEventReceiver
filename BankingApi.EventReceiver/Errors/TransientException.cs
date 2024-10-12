namespace BankingApi.EventReceiver.Errors
{
    public class TransientException : Exception
    {
        public TransientException(string message) : base(message) { }
    }
}
