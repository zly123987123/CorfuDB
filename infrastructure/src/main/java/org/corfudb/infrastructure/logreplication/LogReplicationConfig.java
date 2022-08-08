package org.corfudb.infrastructure.logreplication;

import com.google.common.annotations.VisibleForTesting;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.StreamsDiscoveryMode;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.TableRegistry;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;

/**
 * This class represents any Log Replication Configuration,
 * i.e., set of parameters common across all Clusters.
 */
@Slf4j
@Data
@ToString
public class LogReplicationConfig {

    // Log Replication message timeout time in milliseconds
    public static final int DEFAULT_TIMEOUT_MS = 5000;

    // Log Replication default max number of messages generated at the active cluster for each batch
    public static final int DEFAULT_MAX_NUM_MSG_PER_BATCH = 10;

    // Log Replication default max data message size is 64MB
    public static final int MAX_DATA_MSG_SIZE_SUPPORTED = (64 << 20);

    // Log Replication default max cache number of entries
    // Note: if we want to improve performance for large scale this value should be tuned as it
    // used in snapshot sync to quickly access shadow stream entries, written locally.
    // This value is exposed as a configuration parameter for LR.
    public static final int MAX_CACHE_NUM_ENTRIES = 200;

    // Percentage of log data per log replication message
    public static final int DATA_FRACTION_PER_MSG = 90;

    public static final UUID REGISTRY_TABLE_ID = CorfuRuntime.getStreamID(
            getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME));

    public static final UUID PROTOBUF_TABLE_ID = CorfuRuntime.getStreamID(
            getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME));

    // Set of streams that shouldn't be cleared on snapshot apply phase, as these
    // streams should be the result of "merging" the replicated data (from active) + local data (on standby).
    // For instance, RegistryTable (to avoid losing local opened tables on standby)
    public static final Set<UUID> MERGE_ONLY_STREAMS = new HashSet<>(Arrays.asList(
            REGISTRY_TABLE_ID,
            PROTOBUF_TABLE_ID
    ));

    // Indicate how streamsToReplicate and dataStreamToTagsMap are generated. In STATIC mode they are read from
    // external adapter, while in DYNAMIC mode they are built by querying registry table. It is STATIC by default.
    private StreamsDiscoveryMode streamsDiscoveryMode = StreamsDiscoveryMode.STATIC;

    // LogReplicationConfigManager contains a suite of utility methods for updating LogReplicationConfig
    private LogReplicationConfigManager configManager;

    // Unique identifiers for all streams to be replicated across sites
    private Set<String> streamsToReplicate;

    // Mapping from stream ids to their fully qualified names.
    private Map<UUID, String> streamMap;

    // In DYNAMIC mode, only drop streams that have explicitly set is_federated flag to false
    // If streams have not been opened (no records in registry table), they should still be applied
    private Set<UUID> confirmedNoisyStreams;

    // Streaming tags on Sink (map data stream id to list of tags associated to it)
    private Map<UUID, List<UUID>> dataStreamToTagsMap = new HashMap<>();

    // Snapshot Sync Batch Size(number of messages)
    private int maxNumMsgPerBatch;

    // Max Size of Log Replication Data Message
    private int maxMsgSize;

    // Max Cache number of entries
    private int maxCacheSize;

    /**
     * The max size of data payload for the log replication message.
     */
    private int maxDataSizePerMsg;

    /**
     * Constructor for testing purpose
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     */
    @VisibleForTesting
    public LogReplicationConfig(Set<String> streamsToReplicate) {
        this(streamsToReplicate, DEFAULT_MAX_NUM_MSG_PER_BATCH, MAX_DATA_MSG_SIZE_SUPPORTED, MAX_CACHE_NUM_ENTRIES);
    }

    /**
     * Constructor for testing purpose
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     * @param maxNumMsgPerBatch snapshot sync batch size (number of entries per batch)
     */
    @VisibleForTesting
    public LogReplicationConfig(Set<String> streamsToReplicate, int maxNumMsgPerBatch, int maxMsgSize) {
        this(streamsToReplicate, maxNumMsgPerBatch, maxMsgSize, MAX_CACHE_NUM_ENTRIES);
    }

    /**
     * Constructor exposed to {@link CorfuReplicationDiscoveryService}
     */
    public LogReplicationConfig(LogReplicationConfigManager configManager,
                                int maxNumMsgPerBatch, int maxMsgSize, int cacheSize) {
        this(configManager.getStreamsToReplicate(), maxNumMsgPerBatch, maxMsgSize, cacheSize);
        this.streamsDiscoveryMode = configManager.getStreamsDiscoveryMode();
        this.dataStreamToTagsMap = configManager.getStreamingConfigOnSink();
        this.confirmedNoisyStreams = configManager.getConfirmedNoisyStreams();
        this.configManager = configManager;
    }

    /**
     * Constructor for instantiating LogReplicationConfig fields
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     * @param maxNumMsgPerBatch snapshot sync batch size (number of entries per batch)
     */
    private LogReplicationConfig(Set<String> streamsToReplicate, int maxNumMsgPerBatch, int maxMsgSize, int cacheSize) {
        this.streamsToReplicate = streamsToReplicate;
        this.maxNumMsgPerBatch = maxNumMsgPerBatch;
        this.maxMsgSize = maxMsgSize;
        this.maxCacheSize = cacheSize;
        this.maxDataSizePerMsg = maxMsgSize * DATA_FRACTION_PER_MSG / 100;
        streamMap = new HashMap<>();
        streamsToReplicate.forEach(stream -> streamMap.put(CorfuRuntime.getStreamID(stream), stream));
    }

    /**
     * If the streamsDiscoveryMode is DYNAMIC, some streams to replicate and their stream tags could not be known
     * before they are opened (on Source) / their entries in registry table are applied (on Sink). Therefore, in
     * DYNAMIC mode LogReplicationConfig needs to sync with registry table to avoid data loss.
     */
    public void syncWithRegistry() {
        if (streamsDiscoveryMode.equals(StreamsDiscoveryMode.STATIC)) {
            log.warn("No need to sync with registry table in STATIC mode! Please use external adapter instead!");
            return;
        }
        this.streamsToReplicate = configManager.getStreamsToReplicate();
        this.dataStreamToTagsMap = configManager.getStreamingConfigOnSink();
        this.confirmedNoisyStreams = configManager.getConfirmedNoisyStreams();
        streamMap = new HashMap<>();
        streamsToReplicate.forEach(stream -> streamMap.put(CorfuRuntime.getStreamID(stream), stream));
        log.info("Synced with registry table. Ids for streams to replicate: {}, ids for confirmed noisy streams: {}",
                streamMap.keySet(), confirmedNoisyStreams);
    }
}
