package org.corfudb.infrastructure.health;

public enum Component {

    FAILURE_DETECTOR("Failure Detector"),
    COMPACTOR("Compactor"),
    ORCHESTRATOR("Clustering Orchestrator"),
    SEQUENCER("Sequencer"),
    LOG_UNIT("Log Unit"),
    LAYOUT_SERVER("Layout Server");

    private final String fullName;

    Component(String fullName) {
        this.fullName = fullName;
    }

    @Override
    public String toString() {
        return fullName;
    }
}
