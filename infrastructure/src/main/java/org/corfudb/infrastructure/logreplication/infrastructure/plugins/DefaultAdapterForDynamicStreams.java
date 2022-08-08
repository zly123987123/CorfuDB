package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.StreamsDiscoveryMode;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A default adapter for LR to test stream discovery in DYNAMIC mode.
 */
public class DefaultAdapterForDynamicStreams implements ILogReplicationConfigAdapter {

    @Override
    public StreamsDiscoveryMode getStreamsDiscoveryMode() {
        return StreamsDiscoveryMode.DYNAMIC;
    }

    /**
     * Returns a set of fully qualified stream names to replicate
     */
    @Override
    public Set<String> fetchStreamsToReplicate() {
        throw new UnsupportedOperationException("In DYNAMIC mode LR should discover streams to replicate dynamically");
    }

    /**
     * Returns configuration for streaming on sink (standby)
     * <p>
     * This configuration consists of a map containing data stream IDs to stream tags
     * Note that: since data is not deserialized we have no access to stream tags corresponding
     * to the replicated data, therefore, this data must be provided by the plugin externally.
     */
    @Override
    public Map<UUID, List<UUID>> getStreamingConfigOnSink() {
        throw new UnsupportedOperationException("In DYNAMIC mode LR should discover streaming config dynamically");
    }

    /**
     * Returns a version string that indicates the version of LR.
     *
     * @return Version string that indicates the version of LR.
     */
    @Override
    public String getVersion() {
        return "version_latest";
    }
}
