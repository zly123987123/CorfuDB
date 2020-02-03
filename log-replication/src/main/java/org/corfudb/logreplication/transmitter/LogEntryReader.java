package org.corfudb.logreplication.transmitter;

import org.corfudb.logreplication.fsm.LogReplicationContext;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.IStreamView;

import java.util.ArrayList;
import java.util.List;

public class LogEntryReader {
    private IStreamView stream;
    private long globalBaseSnapshot;
    private long lastReadAddr;
    private CorfuRuntime rt;
    private final int MAX_BATCH_SIZE = 1000;
    private LogReplicationContext context;

    public LogEntryReader(LogReplicationContext context) {
        this.context = context;
    }

    void initStream() {
        StreamOptions options = StreamOptions.builder()
                .cacheEntries(false)
                .build();
        stream = rt.getStreamsView()
                .getUnsafe(ObjectsView.TRANSACTION_STREAM_ID, options);
    }

    //poll txnStream
    List<ILogData> poll() {
        stream.seek(lastReadAddr + 1);
        return stream.remaining();
    }

    TxMessage generateOneMessage(List<ILogData> entries) {
        TxMessage txMessage = new TxMessage();
        //set metadata
        //set data
        return  txMessage;
    }

    List<TxMessage> nextMsgs() {
        ArrayList entries = new ArrayList(poll());
        List<TxMessage> msgList = new ArrayList<>();

        if (entries.isEmpty()) {
            //need to block?
        }

        for (int i = 0; i < entries.size(); i += MAX_BATCH_SIZE) {
            List<ILogData> msg_entries = entries.subList(i, i + MAX_BATCH_SIZE);
            TxMessage txMsg = generateOneMessage(entries);
            msgList.add(txMsg);
        }

        return msgList;
    }

    public void sync() {
        initStream();
        nextMsgs();
        // call callback to process message
    }
}
