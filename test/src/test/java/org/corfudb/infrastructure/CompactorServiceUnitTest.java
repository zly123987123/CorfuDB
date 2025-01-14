package org.corfudb.infrastructure;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class CompactorServiceUnitTest {
    private final ServerContext serverContext = mock(ServerContext.class);
    private final CorfuRuntime corfuRuntime = mock(CorfuRuntime.class);
    private final InvokeCheckpointingJvm invokeCheckpointingJvm = mock(InvokeCheckpointingJvm.class);
    private final CorfuStore corfuStore = mock(CorfuStore.class);
    private final TxnContext txn = mock(TxnContext.class);
    private final CorfuStoreEntry<? extends Message, ? extends Message, ? extends Message> corfuStoreEntry =
            (CorfuStoreEntry<? extends Message, ? extends Message, ? extends Message>) mock(CorfuStoreEntry.class);
    private final DynamicTriggerPolicy dynamicTriggerPolicy = mock(DynamicTriggerPolicy.class);
    private final CompactorLeaderServices leaderServices = mock(CompactorLeaderServices.class);

    private static final int SCHEDULER_INTERVAL = 1;
    private static final String NODE_ENDPOINT = "NodeEndpoint";
    private static final int SLEEP_WAIT = 8;
    private static final String NODE_0 = "0";
    private static final String SLEEP_INTERRUPTED_EXCEPTION_MSG = "Sleep interrupted";


    @Before
    public void setup() {
        CompactorService compactorService = new CompactorService(serverContext,
                SingletonResource.withInitial(() -> corfuRuntime), invokeCheckpointingJvm, dynamicTriggerPolicy);
        CompactorService compactorServiceSpy = spy(compactorService);

        Map<String, Object> map = new HashMap<>();
        map.put("<port>", "port");
        when(serverContext.getLocalEndpoint()).thenReturn(NODE_ENDPOINT);
        when(serverContext.getServerConfig()).thenReturn(map);

        CorfuRuntime.CorfuRuntimeParameters mockParams = mock(CorfuRuntime.CorfuRuntimeParameters.class);
        when(corfuRuntime.getParameters()).thenReturn(mockParams);
        when(mockParams.getCheckpointTriggerFreqMillis()).thenReturn(1L);

        doReturn(leaderServices).when(compactorServiceSpy).getCompactorLeaderServices();
        doReturn(corfuStore).when(compactorServiceSpy).getCorfuStore();
        compactorServiceSpy.start(Duration.ofSeconds(SCHEDULER_INTERVAL));

        when(corfuStore.txn(CORFU_SYSTEM_NAMESPACE)).thenReturn(txn);
        when(txn.getRecord(Matchers.anyString(), Matchers.any(Message.class))).thenReturn(corfuStoreEntry);
        when(txn.commit()).thenReturn(CorfuStoreMetadata.Timestamp.getDefaultInstance());
    }

    @Test
    public void runOrchestratorNonLeaderTest() {
        Layout mockLayout = mock(Layout.class);
        when(corfuRuntime.invalidateLayout()).thenReturn(CompletableFuture.completedFuture(mockLayout));
        //isLeader becomes false
        when(mockLayout.getPrimarySequencer()).thenReturn(NODE_ENDPOINT + NODE_0);

        when((CheckpointingStatus) corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        when(invokeCheckpointingJvm.isRunning()).thenReturn(false).thenReturn(true);
        when(invokeCheckpointingJvm.isInvoked()).thenReturn(false).thenReturn(true);

        try {
            TimeUnit.SECONDS.sleep(SLEEP_WAIT);
        } catch (InterruptedException e) {
            log.warn(SLEEP_INTERRUPTED_EXCEPTION_MSG, e);
        }

        verify(invokeCheckpointingJvm, times(1)).shutdown();
        verify(invokeCheckpointingJvm).invokeCheckpointing();
    }

    @Test
    public void runOrchestratorLeaderTest() {
        Layout mockLayout = mock(Layout.class);
        when(corfuRuntime.invalidateLayout()).thenReturn(CompletableFuture.completedFuture(mockLayout));
        //isLeader becomes true
        when(mockLayout.getPrimarySequencer()).thenReturn(NODE_ENDPOINT)
                .thenReturn(NODE_ENDPOINT)
                .thenReturn(NODE_ENDPOINT + NODE_0);

        when((CheckpointingStatus) corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        when(dynamicTriggerPolicy.shouldTrigger(Matchers.anyLong(), Matchers.any(CorfuStore.class))).thenReturn(true).thenReturn(false);
        doNothing().when(leaderServices).validateLiveness();
        doReturn(CompactorLeaderServices.LeaderInitStatus.SUCCESS).when(leaderServices).initCompactionCycle();
        when(invokeCheckpointingJvm.isRunning()).thenReturn(false).thenReturn(true);
        when(invokeCheckpointingJvm.isInvoked()).thenReturn(false).thenReturn(true);

        try {
            TimeUnit.SECONDS.sleep(SLEEP_WAIT);
        } catch (InterruptedException e) {
            log.warn(SLEEP_INTERRUPTED_EXCEPTION_MSG, e);
        }

        verify(leaderServices).validateLiveness();
        verify(leaderServices).initCompactionCycle();
        verify(invokeCheckpointingJvm, times(1)).shutdown();
    }
}
