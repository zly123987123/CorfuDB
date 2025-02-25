package org.corfudb.infrastructure;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.RpcCommon;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class DynamicTriggerPolicyUnitTest {

    private final CorfuStore corfuStore = mock(CorfuStore.class);
    private static final long INTERVAL = 1000;

    private DynamicTriggerPolicy dynamicTriggerPolicy;
    private final CorfuStoreEntry<? extends Message, ? extends Message, ? extends Message> corfuStoreEntry =
            (CorfuStoreEntry<? extends Message, ? extends Message, ? extends Message>) mock(CorfuStoreEntry.class);
    private final TxnContext txn = mock(TxnContext.class);

    @Before
    public void setup() {
        this.dynamicTriggerPolicy = new DynamicTriggerPolicy();

        when(corfuStore.txn(Matchers.any())).thenReturn(txn);
        when(txn.getRecord(Matchers.anyString(), Matchers.any(Message.class))).thenReturn(corfuStoreEntry);
        when(txn.commit()).thenReturn(CorfuStoreMetadata.Timestamp.getDefaultInstance());
    }

    @Test
    public void testShouldTrigger() {
        //this makes shouldForceTrigger and isCheckpointFrozen to return false
        when(corfuStoreEntry.getPayload()).thenReturn(null);

        dynamicTriggerPolicy.markCompactionCycleStart();
        assert !dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore);

        try {
            TimeUnit.MILLISECONDS.sleep(INTERVAL * 2);
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted: ", e);
        }
        assert dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore);
    }

    @Test
    public void testShouldForceTrigger() {
        when((RpcCommon.TokenMsg) corfuStoreEntry.getPayload()).thenReturn(null)
                .thenReturn(RpcCommon.TokenMsg.getDefaultInstance());
        assert dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore);
    }

    @Test
    public void testCheckpointFrozen() {
        when((RpcCommon.TokenMsg) corfuStoreEntry.getPayload()).thenReturn(RpcCommon.TokenMsg.newBuilder()
                .setSequence(System.currentTimeMillis()).build());
        assert !dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore);
    }
}
