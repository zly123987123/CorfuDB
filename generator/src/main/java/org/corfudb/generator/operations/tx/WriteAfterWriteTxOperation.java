package org.corfudb.generator.operations.tx;

import org.corfudb.generator.Correctness;
import org.corfudb.generator.state.State;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

/**
 * Created by rmichoud on 10/6/17.
 */
public class WriteAfterWriteTxOperation extends AbstractTxOperation {

    public WriteAfterWriteTxOperation(State state) {
        super(state, "TxWaw");
    }

    @Override
    public void execute() {
        Correctness.recordTransactionMarkers(false, shortName, Correctness.TX_START);
        long timestamp;
        state.startWriteAfterWriteTx();

        executeOperations();
        try {
            timestamp = state.stopTx();
            Correctness.recordTransactionMarkers(true, shortName, Correctness.TX_END,
                    Long.toString(timestamp));
        } catch (TransactionAbortedException tae) {
            Correctness.recordTransactionMarkers(false, shortName, Correctness.TX_ABORTED);
        }

    }
}
