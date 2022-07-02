package org.corfudb.infrastructure;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.*;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.QuotaExceededException;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class CompactorServiceTest extends AbstractViewTest {

    ServerContext sc0;
    ServerContext sc1;
    ServerContext sc2;

    private static final Duration LIVENESS_TIMEOUT = Duration.ofMillis(5000);
    private static final int WAIT_TO_KILL = 3000;
    private static final int COMPACTOR_SERVICE_INTERVAL = 1000;

    private static final String CACHE_SIZE_HEAP_RATIO = "0.0";
    private static final String CLIENT_NAME_PREFIX = "Client";
    private static final String OPEN_TABLES_EXCEPTION_MSG = "Exception while opening tables";
    private static final String SLEEP_INTERRUPTED_EXCEPTION_MSG = "Sleep interrupted";

    private CorfuRuntime runtime0 = null;
    private CorfuRuntime runtime1 = null;
    private CorfuRuntime runtime2 = null;
    private CorfuRuntime cpRuntime0 = null;
    private CorfuRuntime cpRuntime1 = null;
    private CorfuRuntime cpRuntime2 = null;

    private DistributedCheckpointer distributedCheckpointer0;
    private DistributedCheckpointer distributedCheckpointer1;
    private DistributedCheckpointer distributedCheckpointer2;


    private CorfuStore corfuStore = null;

    private final Set<String> clientIds = new HashSet<>();
    private final Map<String, Table<StringKey, StringKey, Message>> openedStreams = new HashMap<>();

    private static final String STREAM_NAME_PREFIX = "StreamName";
    private static final String STREAM_KEY_PREFIX = "StreamKey";
    private static final long CHECKPOINT_TRIGGER_FREQ_MS = TimeUnit.MINUTES.toMillis(5);
    private static final int ITEM_SIZE = 10000;
    private static final int NUM_RECORDS = 100;

    private static final Double logSizeLimitPercentageFull = 100.0;
    private static final Double logSizeLimitPercentageLow = 0.0002;

    private final InvokeCheckpointingJvm mockInvokeJvm0 = Mockito.mock(InvokeCheckpointingJvm.class);
    private final InvokeCheckpointingJvm mockInvokeJvm1 = Mockito.mock(InvokeCheckpointingJvm.class);
    private final InvokeCheckpointingJvm mockInvokeJvm2 = Mockito.mock(InvokeCheckpointingJvm.class);

    /**
     * Generates and bootstraps a 3 node cluster in disk mode.
     * Shuts down the management servers of the 3 nodes.
     *
     * @return The generated layout.
     */
    private Layout setup3NodeCluster(Double logSizeLimitPercentage) {
        sc0 = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(SERVERS.PORT_0))
                .setPort(SERVERS.PORT_0)
                .setMemory(false)
                .setCacheSizeHeapRatio(CACHE_SIZE_HEAP_RATIO)
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .setLogSizeLimitPercentage(Double.toString(logSizeLimitPercentage))
                .build();
        sc1 = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(SERVERS.PORT_1))
                .setPort(SERVERS.PORT_1)
                .setMemory(false)
                .setCacheSizeHeapRatio(CACHE_SIZE_HEAP_RATIO)
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .setLogSizeLimitPercentage(Double.toString(logSizeLimitPercentage))
                .build();
        sc2 = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(SERVERS.PORT_2))
                .setPort(SERVERS.PORT_2)
                .setMemory(false)
                .setCacheSizeHeapRatio(CACHE_SIZE_HEAP_RATIO)
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .setLogSizeLimitPercentage(Double.toString(logSizeLimitPercentage))
                .build();

        addServer(SERVERS.PORT_0, sc0);
        addServer(SERVERS.PORT_1, sc1);
        addServer(SERVERS.PORT_2, sc2);
        Layout l = new TestLayoutBuilder()
                .setEpoch(0L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .setReplicationMode(Layout.ReplicationMode.CHAIN_REPLICATION)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();

        bootstrapAllServers(l);
        return l;
    }

    public void testSetup(Double logSizeLimitPercentage) {
        Layout layout = setup3NodeCluster(logSizeLimitPercentage);
        runtime0 = getRuntime(layout).connect();
        runtime1 = getRuntime(layout).connect();
        runtime2 = getRuntime(layout).connect();
        runtime0.getParameters().setPriorityLevel(CorfuMessage.PriorityLevel.HIGH);
        runtime0.getParameters().setClientName(CLIENT_NAME_PREFIX + "0");
        runtime1.getParameters().setClientName(CLIENT_NAME_PREFIX + "1");
        runtime2.getParameters().setClientName(CLIENT_NAME_PREFIX + "2");
        runtime0.getParameters().setCheckpointTriggerFreqMillis(CHECKPOINT_TRIGGER_FREQ_MS);
        runtime1.getParameters().setCheckpointTriggerFreqMillis(CHECKPOINT_TRIGGER_FREQ_MS);
        runtime2.getParameters().setCheckpointTriggerFreqMillis(CHECKPOINT_TRIGGER_FREQ_MS);

        cpRuntime0 = getRuntime(layout).connect();
        cpRuntime1 = getRuntime(layout).connect();
        cpRuntime2 = getRuntime(layout).connect();
        cpRuntime0.getParameters().setClientName(CLIENT_NAME_PREFIX + "_cp0");
        cpRuntime1.getParameters().setClientName(CLIENT_NAME_PREFIX + "_cp1");
        cpRuntime2.getParameters().setClientName(CLIENT_NAME_PREFIX + "_cp2");

        corfuStore = new CorfuStore(runtime0);

        setupMocks();
    }

    private void setupMocks() {
        distributedCheckpointer0 = new ServerTriggeredCheckpointer(CheckpointerBuilder.builder()
                .corfuRuntime(runtime0).cpRuntime(Optional.of(cpRuntime0)).persistedCacheRoot(Optional.empty())
                .isClient(false).build());
        distributedCheckpointer1 = new ServerTriggeredCheckpointer(CheckpointerBuilder.builder()
                .corfuRuntime(runtime1).cpRuntime(Optional.of(cpRuntime1)).persistedCacheRoot(Optional.empty())
                .isClient(false).build());
        distributedCheckpointer2 = new ServerTriggeredCheckpointer(CheckpointerBuilder.builder()
                .corfuRuntime(runtime2).cpRuntime(Optional.of(cpRuntime2)).persistedCacheRoot(Optional.empty())
                .isClient(false).build());

        doAnswer(invocation -> {
            distributedCheckpointer0.checkpointTables();
            return null;
        }).when(mockInvokeJvm0).invokeCheckpointing();
        when(mockInvokeJvm0.isInvoked()).thenReturn(false).thenReturn(true);

        doAnswer(invocation -> {
            distributedCheckpointer1.checkpointTables();
            return null;
        }).when(mockInvokeJvm1).invokeCheckpointing();
        when(mockInvokeJvm1.isInvoked()).thenReturn(false).thenReturn(true);

        doAnswer(invocation -> {
            distributedCheckpointer2.checkpointTables();
            return null;
        }).when(mockInvokeJvm2).invokeCheckpointing();
        when(mockInvokeJvm2.isInvoked()).thenReturn(false).thenReturn(true);
    }

    private Table<StringKey, CheckpointingStatus, Message> openCompactionManagerTable(CorfuStore corfuStore) {
        try {
            return corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    StringKey.class,
                    CheckpointingStatus.class,
                    null,
                    TableOptions.fromProtoSchema(CheckpointingStatus.class));
        } catch (Exception e) {
            log.error(OPEN_TABLES_EXCEPTION_MSG, e);
            return null;
        }
    }

    private Table<TableName, CheckpointingStatus, Message> openCheckpointStatusTable() {
        try {
            return corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME,
                    TableName.class,
                    CheckpointingStatus.class,
                    null,
                    TableOptions.fromProtoSchema(CheckpointingStatus.class));
        } catch (Exception e) {
            log.error(OPEN_TABLES_EXCEPTION_MSG, e);
            return null;
        }
    }

    private Table<TableName, ActiveCPStreamMsg, Message> openActiveCheckpointsTable() {
        try {
            return corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME,
                    TableName.class,
                    ActiveCPStreamMsg.class,
                    null,
                    TableOptions.fromProtoSchema(ActiveCPStreamMsg.class));
        } catch (Exception e) {
            log.error(OPEN_TABLES_EXCEPTION_MSG, e);
            return null;
        }
    }

    private Table<StringKey, RpcCommon.TokenMsg, Message> openCheckpointTable() {
        try {
            return corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    CompactorMetadataTables.CHECKPOINT,
                    StringKey.class,
                    RpcCommon.TokenMsg.class,
                    null,
                    TableOptions.fromProtoSchema(RpcCommon.TokenMsg.class));
        } catch (Exception e) {
            log.error(OPEN_TABLES_EXCEPTION_MSG, e);
            return null;
        }
    }

    private boolean verifyManagerStatus(StatusType targetStatus) {
        openCompactionManagerTable(corfuStore);
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();
            log.info("ManagerStatus: " + managerStatus);
            if (managerStatus.getStatus() == targetStatus) {
                return true;
            }
        }
        return false;
    }

    private boolean verifyCheckpointStatusTable(StatusType targetStatus, int maxFailedTables) {
        Table<TableName, CheckpointingStatus, Message> cpStatusTable = openCheckpointStatusTable();
        int failed = 0;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            List<TableName> tableNames = new ArrayList<>(txn.keySet(cpStatusTable)
                    .stream().collect(Collectors.toList()));
            for (TableName table : tableNames) {
                CheckpointingStatus cpStatus = (CheckpointingStatus) txn.getRecord(
                        CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME, table).getPayload();
                log.info("{} : {} clientId: {}", table.getTableName(), cpStatus.getStatus(), cpStatus.getClientName());
                clientIds.add(cpStatus.getClientName());
                if (cpStatus.getStatus() != targetStatus) {
                    failed++;
                }
            }
            return failed <= maxFailedTables;
        }
    }

    private long verifyCheckpointTable(StringKey targetRecord) {
        openCheckpointTable();
        RpcCommon.TokenMsg token;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            token = (RpcCommon.TokenMsg) txn.getRecord(CompactorMetadataTables.CHECKPOINT, targetRecord).getPayload();
            txn.commit();
        }
        log.info("VerifyCheckpointTable Token: {}", token == null ? "null" : token.toString());

        return token != null ? token.getSequence() : 0;
    }

    private boolean pollForFinishCheckpointing() {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();
            txn.commit();
            if (managerStatus != null && (managerStatus.getStatus() == StatusType.COMPLETED
                    || managerStatus.getStatus() == StatusType.FAILED)) {
                log.info("done pollForFinishCp: {}", managerStatus.getStatus());
                return true;
            }
        }
        return false;
    }

    private Table<StringKey, StringKey, Message> openStream(String streamName) {
        try {
            Table<StringKey, StringKey, Message> table = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    streamName,
                    StringKey.class,
                    StringKey.class,
                    null,
                    TableOptions.fromProtoSchema(StringKey.class));
            openedStreams.put(streamName, table);
            return table;
        } catch (Exception e) {
            log.error(OPEN_TABLES_EXCEPTION_MSG, e);
        }
        return null;
    }

    private void populateStream(String streamName, int numRecords) {
        CorfuStore localCorfuStore = new CorfuStore(runtime2);
        for (int i = 0; i < numRecords; i++) {
            try (TxnContext txn = localCorfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                byte[] array = new byte[ITEM_SIZE];
                new Random().nextBytes(array);
                txn.putRecord(openedStreams.get(streamName),
                        StringKey.newBuilder().setKey(STREAM_KEY_PREFIX + i).build(),
                        StringKey.newBuilder().setKey(new String(array, StandardCharsets.UTF_16)).build(),
                        null);
                txn.commit();
            }
        }
    }

    @Test
    public void singleServerTest() {
        testSetup(logSizeLimitPercentageFull);
        SingletonResource<CorfuRuntime> runtimeSingletonResource1 = SingletonResource.withInitial(() -> runtime0);
        CompactorLeaderServices.setLivenessTimeout(LIVENESS_TIMEOUT);

        try (MockedConstruction<DynamicTriggerPolicy> mockTriggerPolicyConstruction =
                     mockConstruction(DynamicTriggerPolicy.class)) {
            CompactorService compactorService1 = new CompactorService(sc0, runtimeSingletonResource1, mockInvokeJvm0);
            compactorService1.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
            DynamicTriggerPolicy mockTriggerPolicy = mockTriggerPolicyConstruction.constructed().get(0);
            when(mockTriggerPolicy.shouldTrigger(anyLong())).thenReturn(true).thenReturn(false);
        }

        try {
            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(COMPACTOR_SERVICE_INTERVAL);
            }
        } catch (InterruptedException e) {
            log.warn(SLEEP_INTERRUPTED_EXCEPTION_MSG, e);
        }

        assert verifyManagerStatus(StatusType.COMPLETED);
        assert verifyCheckpointStatusTable(StatusType.COMPLETED, 0);
        assert verifyCheckpointTable(CompactorMetadataTables.CHECKPOINT_KEY) > 0;
    }

    @Test
    public void multipleServerTest() {
        testSetup(logSizeLimitPercentageFull);
        CompactorLeaderServices.setLivenessTimeout(LIVENESS_TIMEOUT);

        SingletonResource<CorfuRuntime> runtimeSingletonResource1 = SingletonResource.withInitial(() -> runtime0);
        SingletonResource<CorfuRuntime> runtimeSingletonResource2 = SingletonResource.withInitial(() -> runtime1);

        try (MockedConstruction<DynamicTriggerPolicy> mockTriggerPolicyConstruction = mockConstruction(DynamicTriggerPolicy.class)) {
            CompactorService compactorService1 = new CompactorService(sc0, runtimeSingletonResource1, mockInvokeJvm0);
            compactorService1.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
            DynamicTriggerPolicy mockTriggerPolicy = mockTriggerPolicyConstruction.constructed().get(0);
            when(mockTriggerPolicy.shouldTrigger(anyLong())).thenReturn(true).thenReturn(false);

            CompactorService compactorService2 = new CompactorService(sc1, runtimeSingletonResource2, mockInvokeJvm1);
            compactorService2.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
            mockTriggerPolicy = mockTriggerPolicyConstruction.constructed().get(1);
            when(mockTriggerPolicy.shouldTrigger(anyLong())).thenReturn(false);
        }

        try {
            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(COMPACTOR_SERVICE_INTERVAL);
            }
        } catch (InterruptedException e) {
            log.warn(SLEEP_INTERRUPTED_EXCEPTION_MSG, e);
        }

        assert verifyManagerStatus(StatusType.COMPLETED);
        assert verifyCheckpointStatusTable(StatusType.COMPLETED, 0);
        assert verifyCheckpointTable(CompactorMetadataTables.CHECKPOINT_KEY) > 0;
    }

    @Test
    public void leaderFailureTest() {
        testSetup(logSizeLimitPercentageFull);
        CompactorLeaderServices.setLivenessTimeout(LIVENESS_TIMEOUT);

        SingletonResource<CorfuRuntime> runtimeSingletonResource0 = SingletonResource.withInitial(() -> runtime0);
        CompactorService compactorService0 = new CompactorService(sc0, runtimeSingletonResource0, mockInvokeJvm0);
        SingletonResource<CorfuRuntime> runtimeSingletonResource1 = SingletonResource.withInitial(() -> runtime1);
        CompactorService compactorService1 = new CompactorService(sc1, runtimeSingletonResource1, mockInvokeJvm1);
        SingletonResource<CorfuRuntime> runtimeSingletonResource2 = SingletonResource.withInitial(() -> runtime2);
        CompactorService compactorService2 = new CompactorService(sc2, runtimeSingletonResource2, mockInvokeJvm2);

        try (MockedConstruction<DynamicTriggerPolicy> mockTriggerPolicyConstruction = mockConstruction(DynamicTriggerPolicy.class)) {
            compactorService0.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
            DynamicTriggerPolicy mockTriggerPolicy = mockTriggerPolicyConstruction.constructed().get(0);
            when(mockTriggerPolicy.shouldTrigger(anyLong())).thenReturn(true).thenReturn(false);

            compactorService1.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
            mockTriggerPolicy = mockTriggerPolicyConstruction.constructed().get(1);
            when(mockTriggerPolicy.shouldTrigger(anyLong())).thenReturn(false);

            compactorService2.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
            mockTriggerPolicy = mockTriggerPolicyConstruction.constructed().get(2);
            when(mockTriggerPolicy.shouldTrigger(anyLong())).thenReturn(false);
        }

        try {
            TimeUnit.MILLISECONDS.sleep(WAIT_TO_KILL);
            shutdownServer(SERVERS.PORT_0);
            compactorService0.shutdown();

            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(COMPACTOR_SERVICE_INTERVAL);
            }
        } catch (Exception e) {
            log.warn(SLEEP_INTERRUPTED_EXCEPTION_MSG, e);
        }

        assert verifyManagerStatus(StatusType.COMPLETED);
        assert verifyCheckpointStatusTable(StatusType.COMPLETED, 0);
        assert verifyCheckpointTable(CompactorMetadataTables.CHECKPOINT_KEY) > 0;
    }

    @Test
    public void nonLeaderFailureTest() {
        testSetup(logSizeLimitPercentageFull);
        CompactorLeaderServices.setLivenessTimeout(LIVENESS_TIMEOUT);

        SingletonResource<CorfuRuntime> runtimeSingletonResource0 = SingletonResource.withInitial(() -> runtime0);
        CompactorService compactorService0 = new CompactorService(sc0, runtimeSingletonResource0, mockInvokeJvm0);
        SingletonResource<CorfuRuntime> runtimeSingletonResource1 = SingletonResource.withInitial(() -> runtime1);
        CompactorService compactorService1 = new CompactorService(sc1, runtimeSingletonResource1, mockInvokeJvm1);
        SingletonResource<CorfuRuntime> runtimeSingletonResource2 = SingletonResource.withInitial(() -> runtime2);
        CompactorService compactorService2 = new CompactorService(sc2, runtimeSingletonResource2, mockInvokeJvm2);

        try (MockedConstruction<DynamicTriggerPolicy> mockTriggerPolicyConstruction =
                     mockConstruction(DynamicTriggerPolicy.class)) {
            compactorService0.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
            DynamicTriggerPolicy mockTriggerPolicy = mockTriggerPolicyConstruction.constructed().get(0);
            when(mockTriggerPolicy.shouldTrigger(anyLong())).thenReturn(true).thenReturn(false);

            compactorService1.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
            mockTriggerPolicy = mockTriggerPolicyConstruction.constructed().get(1);
            when(mockTriggerPolicy.shouldTrigger(anyLong())).thenReturn(false);

            compactorService2.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
            mockTriggerPolicy = mockTriggerPolicyConstruction.constructed().get(2);
            when(mockTriggerPolicy.shouldTrigger(anyLong())).thenReturn(false);
        }

        try {
            TimeUnit.MILLISECONDS.sleep(WAIT_TO_KILL);
            shutdownServer(SERVERS.PORT_1);
            compactorService1.shutdown();

            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(COMPACTOR_SERVICE_INTERVAL);
            }
        } catch (Exception e) {
            log.warn("Exception: ", e);
        }

        assert verifyManagerStatus(StatusType.COMPLETED);
        assert verifyCheckpointStatusTable(StatusType.COMPLETED, 0);
        assert verifyCheckpointTable(CompactorMetadataTables.CHECKPOINT_KEY) > 0;
    }

    @Test
    public void checkpointFailureTest() {
        testSetup(logSizeLimitPercentageFull);
        CompactorLeaderServices.setLivenessTimeout(LIVENESS_TIMEOUT);
        SingletonResource<CorfuRuntime> runtimeSingletonResource1 = SingletonResource.withInitial(() -> runtime0);

        try (MockedConstruction<DynamicTriggerPolicy> mockTriggerPolicyConstruction = mockConstruction(DynamicTriggerPolicy.class)) {
            CompactorService compactorService1 = new CompactorService(sc0, runtimeSingletonResource1, mockInvokeJvm0);
            compactorService1.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
            DynamicTriggerPolicy mockTriggerPolicy = mockTriggerPolicyConstruction.constructed().get(0);
            when(mockTriggerPolicy.shouldTrigger(anyLong())).thenReturn(true).thenReturn(false);
        }

        try {
            TimeUnit.MILLISECONDS.sleep(LIVENESS_TIMEOUT.toMillis() / 2);
            Table<TableName, ActiveCPStreamMsg, Message> activeCheckpointTable = openActiveCheckpointsTable();
            Table<TableName, CheckpointingStatus, Message> checkpointStatusTable = openCheckpointStatusTable();
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                TableName table = TableName.newBuilder().setNamespace(CORFU_SYSTEM_NAMESPACE).setTableName(STREAM_NAME_PREFIX).build();
                //Adding a table with STARTED value - making it look like someone started and died while checkpointing
                txn.putRecord(checkpointStatusTable, table,
                        CheckpointingStatus.newBuilder().setStatusValue(StatusType.STARTED_VALUE).build(), null);
                txn.putRecord(activeCheckpointTable,
                        table,
                        ActiveCPStreamMsg.getDefaultInstance(),
                        null);
                txn.commit();
                log.info("Added stream table*****");
            }

            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(COMPACTOR_SERVICE_INTERVAL);
            }
        } catch (InterruptedException e) {
            log.warn(SLEEP_INTERRUPTED_EXCEPTION_MSG, e);
        }

        assert verifyManagerStatus(StatusType.FAILED);
        assert verifyCheckpointStatusTable(StatusType.COMPLETED, 1);
    }

    @Test
    public void quotaExceededTest() {
        testSetup(logSizeLimitPercentageLow);
        CompactorLeaderServices.setLivenessTimeout(LIVENESS_TIMEOUT);
        SingletonResource<CorfuRuntime> runtimeSingletonResource1 = SingletonResource.withInitial(() -> runtime0);

        CompactorService compactorService1 = new CompactorService(sc0, runtimeSingletonResource1, mockInvokeJvm0);
        try (MockedConstruction<DynamicTriggerPolicy> mockTriggerPolicyConstruction = mockConstruction(DynamicTriggerPolicy.class)) {
            compactorService1.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
            DynamicTriggerPolicy mockTriggerPolicy = mockTriggerPolicyConstruction.constructed().get(0);
            when(mockTriggerPolicy.shouldTrigger(anyLong())).thenReturn(true).thenReturn(false);
        }

        openStream(STREAM_NAME_PREFIX);
        Exception ex = assertThrows(RuntimeException.class,
                () -> populateStream(STREAM_NAME_PREFIX, NUM_RECORDS));
        assertThat(ex.getCause().getClass()).isEqualTo(QuotaExceededException.class);

        try {
            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(COMPACTOR_SERVICE_INTERVAL);
            }
        } catch (InterruptedException e) {
            log.warn(SLEEP_INTERRUPTED_EXCEPTION_MSG, e);
        }

        assert verifyManagerStatus(StatusType.COMPLETED);
        assert verifyCheckpointStatusTable(StatusType.COMPLETED, 0);
        assert verifyCheckpointTable(CompactorMetadataTables.CHECKPOINT_KEY) > 0;
    }

    @Test
    public void upgradeTest() {
        testSetup(logSizeLimitPercentageFull);
        CompactorLeaderServices.setLivenessTimeout(LIVENESS_TIMEOUT);
        SingletonResource<CorfuRuntime> runtimeSingletonResource0 = SingletonResource.withInitial(() -> runtime0);
        CompactorService compactorService0 = new CompactorService(sc0, runtimeSingletonResource0, mockInvokeJvm0);
        compactorService0.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));

        SingletonResource<CorfuRuntime> runtimeSingletonResource1 = SingletonResource.withInitial(() -> runtime1);
        CompactorService compactorService1 = new CompactorService(sc1, runtimeSingletonResource1, mockInvokeJvm1);
        compactorService1.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));

        Table<StringKey, RpcCommon.TokenMsg, Message> checkpointTable = openCheckpointTable();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            txn.putRecord(checkpointTable,
                    CompactorMetadataTables.UPGRADE_KEY,
                    RpcCommon.TokenMsg.getDefaultInstance(),
                    null);
            txn.commit();
        }

        try {
            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(COMPACTOR_SERVICE_INTERVAL);
            }
        } catch (InterruptedException e) {
            log.warn(SLEEP_INTERRUPTED_EXCEPTION_MSG, e);
        }

        long trimSequence = verifyCheckpointTable(CompactorMetadataTables.CHECKPOINT_KEY);

        assert verifyManagerStatus(StatusType.COMPLETED);
        assert verifyCheckpointStatusTable(StatusType.COMPLETED, 0);
        assert trimSequence > 0;
        assert verifyCheckpointTable(CompactorMetadataTables.UPGRADE_KEY) == 0;
        assert runtimeSingletonResource0.get().getAddressSpaceView().getTrimMark().getSequence() == trimSequence + 1;
    }
}
