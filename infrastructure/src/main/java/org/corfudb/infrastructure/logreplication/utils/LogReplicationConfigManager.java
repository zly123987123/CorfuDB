package org.corfudb.infrastructure.logreplication.utils;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ILogReplicationConfigAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.StreamsDiscoveryMode;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.utils.CommonTypes.Uuid;
import org.corfudb.utils.LogReplicationStreams.VersionString;
import org.corfudb.utils.LogReplicationStreams.Version;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.MERGE_ONLY_STREAMS;
import static org.corfudb.runtime.view.ObjectsView.LOG_REPLICATOR_STREAM_INFO;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;

/**
 * Handle creation and maintenance of the Corfu table/s containing names of tables
 * to be replicated.
 * @author pankti-m
 */
@Slf4j
public class LogReplicationConfigManager {

    public static final String LOG_REPLICATION_PLUGIN_VERSION_TABLE = "LogReplicationPluginVersion";
    public static final String VERSION_PLUGIN_KEY = "VERSION";
    private static final String EMPTY_STR = "";

    private ILogReplicationConfigAdapter logReplicationConfigAdapter;

    private final String pluginConfigFilePath;

    private final VersionString versionString = VersionString.newBuilder().setName(VERSION_PLUGIN_KEY).build();

    private final CorfuRuntime rt;

    private final CorfuStore corfuStore;

    private static final Uuid defaultMetadata =
        Uuid.newBuilder().setLsb(0).setMsb(0).build();

    @Getter
    private static String currentVersion;

    private Table<VersionString, Version, Uuid> pluginVersionTable;

    /**
     * Used for testing purpose only.
     */
    @VisibleForTesting
    public LogReplicationConfigManager(CorfuRuntime runtime) {
        this.rt = runtime;
        this.corfuStore = new CorfuStore(runtime);
        this.pluginConfigFilePath = EMPTY_STR;
    }

    public LogReplicationConfigManager(CorfuRuntime runtime, String pluginConfigFilePath) {
        this.rt = runtime;
        this.pluginConfigFilePath = pluginConfigFilePath;
        this.corfuStore = new CorfuStore(runtime);

        initStreamNameFetcherPlugin();
        setupVersionTable();
    }

    public StreamsDiscoveryMode getStreamsDiscoveryMode() {
        return logReplicationConfigAdapter.getStreamsDiscoveryMode();
    }


    private void initStreamNameFetcherPlugin() {
        log.info("Plugin :: {}", pluginConfigFilePath);
        LogReplicationPluginConfig config = new LogReplicationPluginConfig(pluginConfigFilePath);
        File jar = new File(config.getStreamFetcherPluginJARPath());
        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class plugin = Class.forName(config.getStreamFetcherClassCanonicalName(), true, child);
            logReplicationConfigAdapter = (ILogReplicationConfigAdapter) plugin.getDeclaredConstructor()
                    .newInstance();
        } catch (Exception e) {
            log.error("Fatal error: Failed to get Stream Fetcher Plugin", e);
            throw new UnrecoverableCorfuError(e);
        }
    }


    /**
     * Get streams to replicate.
     *
     * In STATIC mode it will be fetched from external plugin.
     * In DYNAMIC mode it will be fetched from registry table.
     *
     * @return Set of fully qualified names of streams to replicate
     */
    public Set<String> getStreamsToReplicate() {
        Set<String> streams;
        if (getStreamsDiscoveryMode().equals(StreamsDiscoveryMode.STATIC)) {
            streams = logReplicationConfigAdapter.fetchStreamsToReplicate();
        } else {
            streams = readStreamsToReplicateFromRegistry();
        }
        // Add registryTable to the streams
        String registryTable = getFullyQualifiedTableName(
                CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME);
        streams.add(registryTable);

        // Add protoBufDescriptorTable to the streams
        String protoTable = getFullyQualifiedTableName(
                CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME);
        streams.add(protoTable);
        return streams;
    }

    /**
     * Read streams to replicate from registry table
     *
     * @return Set of fully qualified stream names of those for log replication
     */
    private Set<String> readStreamsToReplicateFromRegistry() {
        Set<String> streamNameSet = new HashSet<>();
        CorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>> registryTable =
                rt.getTableRegistry().getRegistryTable();
        // Get streams to replicate from registry table and add to streamNameSet.
        registryTable.entryStream()
                .filter(entry -> entry.getValue().getMetadata().getTableOptions().getIsFederated())
                .map(entry -> getFullyQualifiedTableName(entry.getKey()))
                .forEachOrdered(streamNameSet::add);

        return streamNameSet;
    }

    /**
     * Get stream tags to send data change notifications to on the receiver (sink side)
     *
     * In STATIC mode, stream tags will be read from a static configuration file. This file should contain not only
     * the stream tag of interest (namespace, tag), i.e., the stream tag we wish to receive notifications on
     * but also the table names of interest within that tag.
     *
     * In DYNAMIC mode, stream tags are read from registry table. Note that in DYNAMIC mode some streams' tags could
     * not be known before registry table is updated during log replication. In that case, LogReplicationConfig needs
     * to sync with registry table to avoid data loss.
     *
     * @return map of stream tag UUID to data streams UUIDs.
     */
    public Map<UUID, List<UUID>> getStreamingConfigOnSink() {
        Map<UUID, List<UUID>> streamingConfig;
        if (getStreamsDiscoveryMode().equals(StreamsDiscoveryMode.STATIC)) {
            // In STATIC mode, stream tags map is fetched from external adapter
            streamingConfig = logReplicationConfigAdapter.getStreamingConfigOnSink();
        } else {
            // In DYNAMIC mode, stream tags map is built by querying registry table
            streamingConfig = readStreamingConfigFromRegistry();
        }

        // Add stream tags for merge only streams
        for (UUID id : MERGE_ONLY_STREAMS) {
            streamingConfig.put(id,
                    Collections.singletonList(LOG_REPLICATOR_STREAM_INFO.getStreamId()));
        }
        return streamingConfig;
    }

    /**
     * Read streams-tags map from registry table.
     *
     * @return map of stream tag UUID to data streams UUIDs.
     */
    private Map<UUID, List<UUID>> readStreamingConfigFromRegistry() {
        Map<UUID, List<UUID>> streamingConfig = new HashMap<>();
        CorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>> registryTable =
                rt.getTableRegistry().getRegistryTable();

        registryTable.forEach((tableName, tableRecord) -> {
            UUID streamId = CorfuRuntime.getStreamID(getFullyQualifiedTableName(tableName));
            streamingConfig.putIfAbsent(streamId, new ArrayList<>());
            streamingConfig.get(streamId).addAll(
                    tableRecord.getMetadata()
                            .getTableOptions()
                            .getStreamTagList()
                            .stream()
                            .map(streamTag -> TableRegistry.getStreamIdForStreamTag(
                                    tableName.getNamespace(), streamTag))
                            .collect(Collectors.toList()));
        });
        return streamingConfig;
    }

    /**
     * In DYNAMIC mode, only drop streams that have explicitly set is_federated flag to false
     * If streams have not been opened (no records in registry table), they should still be applied
     *
     * @return Set of streams ids for those have explicitly set is_federated flag to false
     */
    public Set<UUID> getConfirmedNoisyStreams() {
        Set<UUID> noisyStreams = new HashSet<>();
        CorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>> registryTable =
                rt.getTableRegistry().getRegistryTable();
        // Get streams to replicate from registry table and add to streamNameSet.
        registryTable.entryStream()
                .filter(entry -> !entry.getValue().getMetadata().getTableOptions().getIsFederated())
                .map(entry -> CorfuRuntime.getStreamID(getFullyQualifiedTableName(entry.getKey())))
                .filter(streamId -> !MERGE_ONLY_STREAMS.contains(streamId))
                .forEachOrdered(noisyStreams::add);

        return noisyStreams;
    }

    /**
     * Initiate version table during constructing LogReplicationConfigManager for upcoming version checks.
     */
    private void setupVersionTable() {
        try {
            pluginVersionTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    LOG_REPLICATION_PLUGIN_VERSION_TABLE, VersionString.class,
                    Version.class, Uuid.class, TableOptions.builder().build());
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening version table", e);
            throw new UnrecoverableCorfuError(e);
        }

        try {
            currentVersion = logReplicationConfigAdapter.getVersion();

            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    VersionResult result = verifyVersionResult(txn);
                    if (result.equals(VersionResult.UNSET) || result.equals(VersionResult.CHANGE)) {
                        // Case of upgrade or initial boot: sync version table with plugin info
                        boolean isUpgraded = result.equals(VersionResult.CHANGE);
                        log.info("Current version from plugin = {}, isUpgraded = {}", currentVersion, isUpgraded);
                        // Persist upgrade flag so a snapshot-sync is enforced upon negotiation
                        // (when it is set to true)
                        Version version = Version.newBuilder()
                                .setVersion(currentVersion)
                                .setIsUpgraded(isUpgraded)
                                .build();
                        txn.putRecord(pluginVersionTable, versionString, version, defaultMetadata);
                    }
                    txn.commit();
                    return null;
                } catch (TransactionAbortedException e) {
                    log.warn("Exception on getStreamsToReplicate()", e);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when updating version table", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    private VersionResult verifyVersionResult(TxnContext txn) {
        CorfuStoreEntry<VersionString, Version, Uuid> record =
                txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString);

        if (record.getPayload() == null) {
            // Initializing
            log.info("LR initializing. Version unset");
            return VersionResult.UNSET;
        } else if (!record.getPayload().getVersion().equals(currentVersion)) {
            // Upgrading
            log.info("LR upgraded. Version changed from {} to {}", currentVersion, record.getPayload().getVersion());
            return VersionResult.CHANGE;
        }

        return VersionResult.SAME;
    }

    /**
     * Helper method for checking whether LR is in upgrading path or not.
     * Note that the default boolean value for ProtoBuf message is false.
     *
     * @return True if LR is in upgrading path, false otherwise.
     */
    public boolean isUpgraded() {
        VersionString versionString = VersionString.newBuilder()
                .setName(VERSION_PLUGIN_KEY).build();
        CorfuStoreEntry<VersionString, Version, Uuid> record;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            record = txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString);
            txn.commit();
        } catch (NoSuchElementException e) {
            // Normally this will not happen as version table should be initialized during bootstrap
            log.error("Version table has not been initialized", e);
            return false;
        }
        return record.getPayload().getIsUpgraded();
    }

    /**
     * Helper method for flipping the boolean flag back to false in version table which
     * indicates the LR upgrading path is complete.
     */
    public void resetUpgradeFlag() {

        log.info("Reset isUpgraded flag");

        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    VersionString versionString = VersionString.newBuilder()
                            .setName(VERSION_PLUGIN_KEY).build();
                    CorfuStoreEntry<VersionString, Version, Uuid> versionEntry =
                            txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString);
                    Version version = Version.newBuilder().mergeFrom(versionEntry.getPayload()).setIsUpgraded(false).build();

                    txn.putRecord(pluginVersionTable, versionString, version, defaultMetadata);
                    txn.commit();
                } catch (TransactionAbortedException e) {
                    log.warn("Exception when resetting upgrade flag in version table", e);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when resetting upgrade flag in version table", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    private enum VersionResult {
        UNSET,
        CHANGE,
        SAME
    }
}
