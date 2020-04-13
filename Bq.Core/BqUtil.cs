using System;
using System.Collections;
using System.Collections.Concurrent;
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

    // "add" to new, get from both. Old gets disposed after ShiftOut
    public class ShiftOutDict<TKey, TValue>
    {
        // new is public so that you can add to id. Old is not
        public ConcurrentDictionary<TKey, TValue> New = new ConcurrentDictionary<TKey, TValue>();
        private ConcurrentDictionary<TKey, TValue> Old = new ConcurrentDictionary<TKey, TValue>();

        public (bool, TValue res) TryGet(TKey key)
        {
            var ok = New.TryGetValue(key, out var res);
            if (ok)
            {
                return (true, res);
            }
            ok = Old.TryGetValue(key, out res);
            if (ok)
            {
                return (true, res);
            }
            return (false, default);
        }

        public void Shift()
        {
            (Old, New) = (New, new ConcurrentDictionary<TKey, TValue>());
        }
    }
}