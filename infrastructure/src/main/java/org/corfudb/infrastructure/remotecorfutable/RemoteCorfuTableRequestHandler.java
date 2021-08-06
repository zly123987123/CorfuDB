package org.corfudb.infrastructure.remotecorfutable;

import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.corfudb.infrastructure.IServerRouter;
import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.CorfuProtocolRemoteCorfuTable.getContainsResponseMsg;
import static org.corfudb.protocols.CorfuProtocolRemoteCorfuTable.getGetResponseMsg;
import static org.corfudb.protocols.CorfuProtocolRemoteCorfuTable.getScanResponseMsg;
import static org.corfudb.protocols.CorfuProtocolRemoteCorfuTable.getSizeRequestMsg;
import static org.corfudb.protocols.CorfuProtocolRemoteCorfuTable.getSizeResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableSizeRequestMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableScanRequestMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableContainsValueRequestMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableContainsKeyRequestMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableGetRequestMsg;
import org.corfudb.runtime.proto.service.RemoteCorfuTable.RemoteCorfuTableRequestMsg;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class RemoteCorfuTableRequestHandler {
    private final DatabaseHandler databaseHandler;

    public RemoteCorfuTableRequestHandler(@NonNull DatabaseHandler databaseHandler) {
        this.databaseHandler = databaseHandler;
    }

    public void handle(@Nonnull RequestMsg req, @Nonnull ChannelHandlerContext ctx,
                       @Nonnull IServerRouter r) {
        RemoteCorfuTableRequestMsg msg = req.getPayload().getRemoteCorfuTableRequest();
        switch (msg.getPayloadCase()) {
            case GET:
                handleGet(req, ctx, r);
                break;
            case SCAN:
                handleScan(req, ctx, r);
                break;
            case CONTAINS_KEY:
                handleContainsKey(req, ctx, r);
                break;
            case CONTAINS_VALUE:
                handleContainsValue(req, ctx, r);
                break;
            case SIZE:
                handleSize(req, ctx, r);
                break;
            default:
                log.error("handle[{}]: Unknown orchestrator request type {}",
                        req.getHeader().getRequestId(), msg.getPayloadCase());
                break;
        }
    }

    private void handleSize(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        RemoteCorfuTableSizeRequestMsg sizeRequestMsg = req.getPayload().getRemoteCorfuTableRequest().getSize();
        UUID streamID = getUUID(sizeRequestMsg.getStreamID());
        long timestamp = sizeRequestMsg.getTimestamp();
        int scanSize = sizeRequestMsg.getInternalScanSize();
        databaseHandler.sizeAsync(streamID, timestamp, scanSize).thenAccept(resultSize -> {
            ResponseMsg response = getResponseMsg(getHeaderMsg(req.getHeader()), getSizeResponseMsg(resultSize));
            r.sendResponse(response, ctx);
        }).exceptionally(ex -> {
            handleException(ex, ctx, req, r);
            return null;
        });
    }

    private void handleContainsValue(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        RemoteCorfuTableContainsValueRequestMsg containsValueRequestMsg = req.getPayload().getRemoteCorfuTableRequest().getContainsValue();
        ByteString payloadValue = containsValueRequestMsg.getPayloadValue();
        UUID streamID = getUUID(containsValueRequestMsg.getStreamID());
        long timestamp = containsValueRequestMsg.getTimestamp();
        int scanSize = containsValueRequestMsg.getInternalScanSize();
        databaseHandler.containsValueAsync(payloadValue, streamID, timestamp, scanSize).thenAccept(contained -> {
            ResponseMsg response = getResponseMsg(getHeaderMsg(req.getHeader()), getContainsResponseMsg(contained));
            r.sendResponse(response, ctx);
        }).exceptionally(ex -> {
            handleException(ex, ctx, req, r);
            return null;
        });
    }

    private void handleContainsKey(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        RemoteCorfuTableContainsKeyRequestMsg containsKeyRequestMsg = req.getPayload().getRemoteCorfuTableRequest().getContainsKey();
        UUID streamID = getUUID(containsKeyRequestMsg.getStreamID());
        RemoteCorfuTableVersionedKey key = new RemoteCorfuTableVersionedKey(
                containsKeyRequestMsg.getVersionedKey().toByteArray());
        databaseHandler.containsKeyAsync(key, streamID).thenAccept(contained -> {
            ResponseMsg response = getResponseMsg(getHeaderMsg(req.getHeader()), getContainsResponseMsg(contained));
            r.sendResponse(response, ctx);
        }).exceptionally(ex -> {
            handleException(ex, ctx, req, r);
            return null;
        });
    }

    private void handleScan(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        RemoteCorfuTableScanRequestMsg scanRequestMsg = req.getPayload().getRemoteCorfuTableRequest().getScan();
        UUID streamID = getUUID(scanRequestMsg.getStreamID());
        int numEntries = scanRequestMsg.getNumEntriesToScan();
        CompletableFuture<List<RemoteCorfuTableEntry>> scanFuture;
        switch (scanRequestMsg.getStartPointCase()) {
            case VERSIONEDSTARTKEY:
                RemoteCorfuTableVersionedKey startKey = new RemoteCorfuTableVersionedKey(
                        scanRequestMsg.getVersionedStartKey().toByteArray());
                if (numEntries == 0) {
                    scanFuture = databaseHandler.scanAsync(startKey, streamID);
                } else {
                    scanFuture = databaseHandler.scanAsync(startKey, numEntries, streamID);
                }

                break;
            case TIMESTAMP:
                long timestamp = scanRequestMsg.getTimestamp();
                if (numEntries == 0) {
                    scanFuture = databaseHandler.scanAsync(streamID, timestamp);
                } else {
                    scanFuture = databaseHandler.scanAsync(numEntries, streamID, timestamp);
                }
                break;
            case STARTPOINT_NOT_SET:
            default:
                handleException(new IllegalStateException("Starting Key or timestamp must be given for SCAN request"),
                        ctx, req, r);
                return;
        }
        scanFuture.thenAccept(scannedEntries -> {
            ResponseMsg responseMsg = getResponseMsg(getHeaderMsg(req.getHeader()),
                    getScanResponseMsg(scannedEntries));
            r.sendResponse(responseMsg, ctx);
        }).exceptionally(ex -> {
            handleException(ex, ctx, req, r);
            return null;
        });
    }

    private void handleGet(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        RemoteCorfuTableGetRequestMsg getRequestMsg = req.getPayload().getRemoteCorfuTableRequest().getGet();
        UUID streamID = getUUID(getRequestMsg.getStreamID());
        RemoteCorfuTableVersionedKey key = new RemoteCorfuTableVersionedKey(
                getRequestMsg.getVersionedKey().toByteArray());
        databaseHandler.getAsync(key, streamID).thenAccept(payloadValue -> {
            ResponseMsg responseMsg = getResponseMsg(getHeaderMsg(req.getHeader()), getGetResponseMsg(payloadValue));
            r.sendResponse(responseMsg, ctx);
        }).exceptionally(ex -> {
            handleException(ex, ctx, req, r);
            return null;
        });
    }

    private void handleException(Throwable ex, ChannelHandlerContext ctx, RequestMsg req, IServerRouter r) {
    }
}
