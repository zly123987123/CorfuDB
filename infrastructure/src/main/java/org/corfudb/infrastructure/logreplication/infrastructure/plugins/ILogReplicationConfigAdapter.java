package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.StreamsDiscoveryMode;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * This Interface must be implemented by any external
 * provider of Log Replication Configuration.
 *
 * Log Replication Configuration encompasses:
 * (1) Streams to replicate
 * (2) System's version
 */
public interface ILogReplicationConfigAdapter {

    /**
     * Indicate the mode to get the set of streams to replicate.
     *
     * STATIC: Streams to replicate are provided by an external plugin file.
     * DYNAMIC: Streams to replicate are dynamically discovered through registry table.
     */
    default StreamsDiscoveryMode getStreamsDiscoveryMode() {
        return StreamsDiscoveryMode.STATIC;
    }

    /**
     * Returns a set of fully qualified stream names to replicate
     */
    Set<String> fetchStreamsToReplicate();

    /**
     * Returns configuration for streaming on sink (standby)
     *
     * This configuration consists of a map containing data stream IDs to stream tags
     * Note that: since data is not deserialized we have no access to stream tags corresponding
     * to the replicated data, therefore, this data must be provided by the plugin externally.
     */
    Map<UUID, List<UUID>> getStreamingConfigOnSink();

    /**
     * Returns a version string that indicates the version of LR.
     *
     * @return Version string that indicates the version of LR.
     */
    String getVersion();
}
