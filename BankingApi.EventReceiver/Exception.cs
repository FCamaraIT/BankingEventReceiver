using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BankingApi.EventReceiver
{
    // Custom exceptions for transient and non-transient errors
    public class TransientException : Exception
    {
        public TransientException(string message) : base(message) { }
        public TransientException(string message, Exception innerException) : base(message, innerException) { }
    }

    public class NonTransientException : Exception
    {
        public NonTransientException(string message) : base(message) { }
    }
}
