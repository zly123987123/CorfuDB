package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.DiscoveryServiceEvent.DiscoveryServiceEventType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.StreamListener;

import java.util.Collections;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.REGISTRY_TABLE_NAME;

@Slf4j
public class CorfuReplicationSubscriberUpdateListener implements StreamListener {

    private LogReplicationConfig replicationConfig;
    private CorfuReplicationDiscoveryService discoveryService;
    private CorfuStore corfuStore;

    public CorfuReplicationSubscriberUpdateListener(CorfuReplicationDiscoveryService discoveryService,
                                                    LogReplicationConfig replicationConfig, CorfuRuntime runtime) {
        this.discoveryService = discoveryService;
        this.replicationConfig = replicationConfig;
        this.corfuStore = new CorfuStore(runtime);
    }

    public void start() {
        log.info("Start listening on the Registry Table");
        try {
            corfuStore.subscribeListener(this, CORFU_SYSTEM_NAMESPACE, "",
                Collections.singletonList(REGISTRY_TABLE_NAME));
        } catch (Exception e) {
            log.error("Failed to subscribe to the Registry table", e);
        }
    }

    public void stop() {
        corfuStore.unsubscribeListener(this);
    }

    @Override
    public void onNext(CorfuStreamEntries results) {
        // TODO: If any update contains a new replication model + client combination, send a callback to discovery
        // service.
        // If an unsupported replication model is found, log an error or warning.
        discoveryService.input(new DiscoveryServiceEvent(DiscoveryServiceEventType.NEW_REPLICATION_SUBSCRIBER,
            (CorfuReplicationSubscriber)null));
        discoveryService.addNewReplicationSubscriber(null);
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error received on Registry Table Listener", throwable);
    }

}
