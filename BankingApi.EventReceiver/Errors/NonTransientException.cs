namespace BankingApi.EventReceiver.Errors
{
    public class NonTransientException : Exception
    {
        public NonTransientException(string message) : base(message) { }
    }
}
