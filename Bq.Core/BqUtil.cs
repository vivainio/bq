using System.Transactions;

namespace Bq
{
    public class BqUtil
    {
        public static TransactionScope Tx() => 
            new TransactionScope(TransactionScopeOption.Required, TransactionScopeAsyncFlowOption.Enabled);
        public static TransactionScope TxNew() => 
            new TransactionScope(TransactionScopeOption.RequiresNew, TransactionScopeAsyncFlowOption.Enabled);

    }
}