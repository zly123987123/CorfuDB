package org.corfudb.logreplication.transmitter;

import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.runtime.CorfuRuntime;

/**
 * This class is responsible of managing the transmission of log entries,
 * i.e, reading and sending incremental updates to a remote site.
 *
 * It reads log entries from the datastore through the LogEntryReader, and sends them
 * through the LogEntryListener (the application specific callback).
 */
public class LogEntryTransmitter {

    /*
     * Corfu Runtime
     */
    private CorfuRuntime runtime;

    /*
     * Implementation of Log Entry Reader. Default implementation reads at the stream layer.
     */
    private LogEntryReader logEntryReader;

    /*
     * Log Entry Listener, application callback to send out reads.
     */
    private LogEntryListener logEntryListener;

    /*
     * Log Replication FSM (to insert internal events)
     */
    private LogReplicationFSM logReplicationFSM;

    private volatile boolean taskActive = false;

    /**
     * Stop the transmit for Log Entry Sync
     */
    public void stop() {
        taskActive = false;
    }

    /**
     * Constructor
     *
     * @param runtime corfu runtime
     * @param logEntryReader log entry reader implementation
     * @param logEntryListener log entry listener implementation (application callback)
     */
    public LogEntryTransmitter(CorfuRuntime runtime, LogEntryReader logEntryReader, LogEntryListener logEntryListener,
                               LogReplicationFSM logReplicationFSM) {
        this.runtime = runtime;
        this.logEntryReader = logEntryReader;
        this.logEntryListener = logEntryListener;
        this.logReplicationFSM = logReplicationFSM;
    }

    /**
     * Read and send incremental updates (log entries)
     */
    public void transmit() {
        taskActive = true;
        while (taskActive) {
            DataMessage message;

            // Read and Send Log Entries
            try {
                message = logEntryReader.read();
                if (logEntryListener.onNext(message)) {
                    // Write meta-data

                } else {
                    // ??
                    // Request full sync (something is wrong I cant deliver)
                    // (Optimization):
                    // Back-off for couple of seconds and retry n times if not require full sync
                }
            } catch (Exception e) {
                // Unrecoverable error, noisy streams found in transaction stream (streams of interest and others not
                // intended for replication). Shutdown.
                logReplicationFSM.input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.REPLICATION_SHUTDOWN));
            }
        }
    }
}