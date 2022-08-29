package org.corfudb.browser;

import com.google.common.collect.Iterables;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.java.Log;
import org.apache.commons.io.FileUtils;
import org.corfudb.infrastructure.log.LogFormat;
import org.corfudb.infrastructure.log.LogFormat.LogEntry;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.DynamicProtobufSerializer;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.corfudb.browser.CorfuStoreBrowserEditor.*;
import static org.corfudb.infrastructure.log.StreamLogFiles.*;


@SuppressWarnings("checkstyle:printLine")
public class CorfuOfflineBrowserEditor implements CorfuBrowserEditorCommands {
    private final Path logDir;
    // make dynamic protobuf serializer final later
    private DynamicProtobufSerializer dynamicProtobufSerializer;
    private final String QUOTE = "\"";
    // concurrent hashmaps registry table and protobufDescriptor tables for materialization

    public CorfuOfflineBrowserEditor(String offlineDbDir) {
        logDir = Paths.get(offlineDbDir, "log");
        System.out.println("Analyzing database located at :"+logDir);

        // for list tables and print all protobufdescriptors
        printLogEntryData();

        // prints a specific table
        printTable("nsx", "LeadershipLease");
    }

    /**
     * Opens all log files one by one, accesses and prints log entry data for each Corfu log file.
     */
    //@param String namespace, String tableName
    public void printLogEntryData() {
        // Add the following lines when the printLogEntryData() method begins...
        CorfuRuntime runtimeWithOnlyProtoSerializer = CorfuRuntime
                .fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build());
        runtimeWithOnlyProtoSerializer.getSerializers()
                .registerSerializer(DynamicProtobufSerializer.createProtobufSerializer());

        System.out.println("Analyzing log information:");

        String[] extension = {"log"};
        File dir = logDir.toFile();
        Collection<File> files = FileUtils.listFiles(dir, extension, true);

        // get the UUIDs of the streams of interest
        String registryTableName = TableRegistry.getFullyQualifiedTableName(TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME);
        UUID registryTableStreamId = CorfuRuntime.getStreamID(registryTableName);

        String protobufDescriptorTableName = TableRegistry.getFullyQualifiedTableName(TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME);
        UUID protobufDescriptorStreamId = CorfuRuntime.getStreamID(protobufDescriptorTableName);

        UUID registryTableCheckpointStream = CorfuRuntime.getCheckpointStreamIdFromId(registryTableStreamId);
        UUID protobufDescriptorCheckpointStream = CorfuRuntime.getCheckpointStreamIdFromId(protobufDescriptorStreamId);

        // temporary lists that store LogEntries in causal order before they are put into the ConcurrentMap
        List<LogEntryOrdering> registryTableEntries = new ArrayList<>();
        List<LogEntryOrdering> protobufDescriptorTableEntries = new ArrayList<>();

        // temporary cached maps
        ConcurrentMap cachedRegistryTable = new ConcurrentHashMap();
        ConcurrentMap cachedProtobufDescriptorTable = new ConcurrentHashMap();

        for (File file : files) {
            try (FileChannel fileChannel = FileChannel.open(file.toPath())) {
                // set the file channel's position back to 0
                fileChannel.position(0);
                //long pos = fileChannel.size();

                // parse header
                LogFormat.LogHeader header = parseHeader(null, fileChannel, file.getAbsolutePath());
                //System.out.println(header);

                // iterate through the file
                // make sure that fileChannel.size() - fileChannel.position() > 14 to prevent
                // actualMetaDataSize < METADATA_SIZE, which would create an exception
                while (fileChannel.size() - fileChannel.position() > 14) {
                    //long channelOffset = fileChannel.position();
                    //System.out.println(channelOffset);

                    // parse metadata and entry
                    LogFormat.Metadata metadata = StreamLogFiles.parseMetadata(null, fileChannel, file.getAbsolutePath());
                    LogEntry entry = StreamLogFiles.parseEntry(null, fileChannel, metadata, file.getAbsolutePath());
                    //System.out.println(metadata);
                    //System.out.println(entry);

                    if(metadata != null && entry != null) {
                        // convert the LogEntry to LogData to access getPayload
                        LogData data = StreamLogFiles.getLogData(entry);
                        //System.out.println(data.getData());

                        processLogData(data, registryTableStreamId, registryTableCheckpointStream, runtimeWithOnlyProtoSerializer, cachedRegistryTable, registryTableEntries);
                        processLogData(data, protobufDescriptorStreamId, protobufDescriptorCheckpointStream, runtimeWithOnlyProtoSerializer, cachedProtobufDescriptorTable, protobufDescriptorTableEntries);
                    }

                }
                System.out.println("Finished processing file: " + file.getAbsolutePath());

            } catch (IOException e) {
                throw new IllegalStateException("Invalid header: " + file.getAbsolutePath(), e);
            }
        }

        //System.out.println(registryTableEntries);
        //System.out.println(protobufDescriptorTableEntries);

        //System.out.println(cachedRegistryTable);
        //System.out.println(cachedProtobufDescriptorTable);

        dynamicProtobufSerializer = new DynamicProtobufSerializer(cachedRegistryTable, cachedProtobufDescriptorTable);

        listTables(null);
        //printAllProtoDescriptors();

        System.out.println("Finished analyzing log information.");
    }

    public void processLogData(LogData data, UUID tableStreamID, UUID tableCheckPointStream, CorfuRuntime runtimeWithOnlyProtoSerializer,
                               ConcurrentMap cachedTable, List<LogEntryOrdering> tableEntries) {
        if(data.containsStream(tableStreamID)
                || data.containsStream(tableCheckPointStream)) {
            // call get payload to decompress and deserialize data
            if(data.getType() == DataType.DATA) {
                Object modifiedData = data.getPayload(runtimeWithOnlyProtoSerializer);
                //System.out.println(modifiedData);

                if(modifiedData instanceof CheckpointEntry) {
                    long snapshotAddress = Long.decode(((CheckpointEntry)modifiedData).getDict().get(CheckpointEntry.CheckpointDictKey.SNAPSHOT_ADDRESS));
                    //System.out.println(snapshotAddress);

                    MultiSMREntry smrEntries = ((CheckpointEntry) modifiedData).getSmrEntries(false, runtimeWithOnlyProtoSerializer);
                    //System.out.println("SMR Entries: " + smrEntries);
                    List<SMREntry> smrUpdates = null;
                    if(smrEntries != null) {
                        smrUpdates = smrEntries.getUpdates();
                        //System.out.println("SMR Updates: " + smrUpdates);
                        for (int i = 0; i < smrUpdates.size(); i++) {
                            LogEntryOrdering entry = new LogEntryOrdering(smrUpdates.get(i), snapshotAddress);
                            tableEntries.add(entry);
                            if (entry.getOrdering() < tableEntries.get(tableEntries.size() - 1).getOrdering()) {
                                Collections.sort(tableEntries, new LogEntryComparator());
                            }
                            if (tableEntries.size() == 15) {
                                addRecordToConcurrentMap(tableEntries, cachedTable);
                            }
                        }
                        if (tableEntries.size() > 0) { // clear the buffer
                            for (int i = 0; i < tableEntries.size(); i++) {
                                addRecordToConcurrentMap(tableEntries, cachedTable);
                            }
                        }
                    }
                }
                else if(modifiedData instanceof MultiObjectSMREntry) {
                    List<SMREntry> smrUpdates = ((MultiObjectSMREntry) modifiedData).getSMRUpdates(tableStreamID);
                    if(smrUpdates != null) {
                        //System.out.println("SMR Updates: " + smrUpdates);
                        for (int i = 0; i < smrUpdates.size(); i++) {
                            LogEntryOrdering entry = new LogEntryOrdering(smrUpdates.get(i), smrUpdates.get(i).getGlobalAddress());
                            tableEntries.add(entry);
                            if (entry.getOrdering() < tableEntries.get(tableEntries.size() - 1).getOrdering()) {
                                Collections.sort(tableEntries, new LogEntryComparator());
                            }
                            if (tableEntries.size() == 15) {
                                addRecordToConcurrentMap(tableEntries, cachedTable);
                            }
                        }
                        if (tableEntries.size() > 0) { // clear the buffer
                            for (int i = 0; i < tableEntries.size(); i++) {
                                addRecordToConcurrentMap(tableEntries, cachedTable);
                            }
                        }
                    }
                }
            }

            else if(data.getType() == DataType.HOLE) {
                System.out.println("Hole found.");
            }
        }
    }

    public void addRecordToConcurrentMap(List<LogEntryOrdering> tableEntries, ConcurrentMap cachedTable) {
        LogEntryOrdering tableEntry = tableEntries.remove(0);
        Object[] smrUpdateArg = ((SMREntry) tableEntry.getObj()).getSMRArguments();
        Object smrUpdateTable = smrUpdateArg[0];
        Object smrUpdateCorfuRecord = smrUpdateArg[1];

        Object corfuRecordTableName = callback(smrUpdateTable);

        CorfuRecord corfuRecord = ((CorfuRecord) smrUpdateCorfuRecord);

        // check which smr method it belongs to: put, clear, or delete
        // and modify table accordingly
        String smrMethod = ((SMREntry) tableEntry.getObj()).getSMRMethod();
        if (smrMethod.equals("put")) {
            cachedTable.put(corfuRecordTableName, corfuRecord);
        } else if (smrMethod.equals("delete")) {
            cachedTable.remove(corfuRecordTableName, corfuRecord);
        } else if (smrMethod.equals("clear")) {
            cachedTable.clear();
        }
    }

    public Object callback(Object smrUpdateTable) {
        if(smrUpdateTable instanceof CorfuStoreMetadata.TableName) {
            return ((CorfuStoreMetadata.TableName) smrUpdateTable);
        } else if(smrUpdateTable instanceof CorfuStoreMetadata.ProtobufFileName) {
            return ((CorfuStoreMetadata.ProtobufFileName) smrUpdateTable);
        }
        return null;
    }

    public void processGenericLogData(LogData data, UUID tableStreamID, UUID tableCheckPointStream, CorfuRuntime runtimeWithOnlyProtoSerializer,
                                      ConcurrentMap cachedTable, List<LogEntryOrdering> tableEntries) {
        if(data.containsStream(tableStreamID)
                || data.containsStream(tableCheckPointStream)) {
            // call get payload to decompress and deserialize data
            if(data.getType() == DataType.DATA) {
                Object modifiedData = data.getPayload(runtimeWithOnlyProtoSerializer);
                //System.out.println(modifiedData);

                if(modifiedData instanceof CheckpointEntry) {
                    MultiSMREntry smrEntries = ((CheckpointEntry) modifiedData).getSmrEntries(false, runtimeWithOnlyProtoSerializer);
                    List<SMREntry> smrUpdates = null;
                    if(smrEntries != null) {
                        smrUpdates = smrEntries.getUpdates();
                        for (int i = 0; i < smrUpdates.size(); i++) {
                            // Get serialized form of arguments for caller's table. They were sent in OpaqueEntry and
                            // need to be deserialized using DynamicProtobufSerializer that is created outside this context
                            Object[] objs = smrUpdates.get(i).getSMRArguments();

                            CorfuDynamicKey key = (CorfuDynamicKey) objs[0];
                            CorfuDynamicRecord record = (CorfuDynamicRecord) objs[1];

                            cachedTable.put(key, record);
                        }
                    }
                }

                // iterate over the cached table and call existing print key, print payload and print metadata methods
                for (Object entry : cachedTable.entrySet()) {
                    printKey((Map.Entry<CorfuDynamicKey, CorfuDynamicRecord>) entry);
                    //Map.Entry<CorfuDynamicKey, CorfuDynamicRecord> entry2 = (Map.Entry<CorfuDynamicKey, CorfuDynamicRecord>) entry;
                    printPayload((Map.Entry<CorfuDynamicKey, CorfuDynamicRecord>) entry);
                    printMetadata((Map.Entry<CorfuDynamicKey, CorfuDynamicRecord>) entry);
                }


                /**
                else if(modifiedData instanceof MultiObjectSMREntry) {
                    List<SMREntry> smrUpdates = ((MultiObjectSMREntry) modifiedData).getSMRUpdates(tableStreamID);
                    if(smrUpdates != null) {
                        for (int i = 0; i < smrUpdates.size(); i++) {
                            // Get serialized form of arguments for caller's table. They were sent in OpaqueEntry and
                            // need to be deserialized using DynamicProtobufSerializer that is created outside this context
                            Object[] objs = smrEntry.getSMRArguments();
                            ByteBuf keyBuf = Unpooled.wrappedBuffer((byte[]) objs[0]);
                            CorfuDynamicKey key = dynamicProtobufSerializer.deserialize(keyBuf, null);

                            ByteBuf valueBuf = Unpooled.wrappedBuffer((byte[]) objs[1]);
                            CorfuDynamicRecord record = (CorfuDynamicRecord) dynamicProtobufSerializer.deserialize(valueBuf, null);

                            cachedTable.put(key, record);
                        }
                    }
                }

                 */
            }

            else if(data.getType() == DataType.HOLE) {
                System.out.println("Hole found.");
            }
        }
    }

    @Override
    public EnumMap<IMetadata.LogUnitMetadataType, Object> printMetadataMap(long address) {
        return null;
    }

    /**
     * Fetches the table from the given namespace
     * @param namespace Namespace of the table
     * @param tableName Tablename
     * @return CorfuTable
     */
    @Override
    public CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> getTable(
            String namespace, String tableName) {
        System.out.println("Namespace: " + namespace);
        System.out.println("TableName: " + tableName);

        String fullTableName = TableRegistry.getFullyQualifiedTableName(namespace, tableName);

        return null;
    }

    /**
     * Prints the payload and metadata in the given table
     * @param namespace - the namespace where the table belongs
     * @param tablename - table name without the namespace
     * @return - number of entries in the table
     */
    @Override
    public int printTable(String namespace, String tablename) {
        // get the UUIDs of the streams of interest
        String genericTableName = TableRegistry.getFullyQualifiedTableName(namespace, tablename);
        UUID genericTableStreamId = CorfuRuntime.getStreamID(genericTableName);
        UUID genericTableCheckpointStream = CorfuRuntime.getCheckpointStreamIdFromId(genericTableStreamId);

        CorfuRuntime runtimeWithOnlyProtoSerializer = CorfuRuntime
                .fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build());
        runtimeWithOnlyProtoSerializer.getSerializers()
                .registerSerializer(dynamicProtobufSerializer);

        System.out.println("Analyzing log information:");

        String[] extension = {"log"};
        File dir = logDir.toFile();
        Collection<File> files = FileUtils.listFiles(dir, extension, true);
        List<LogEntryOrdering> genericTableEntries = new ArrayList<>();
        ConcurrentMap<CorfuDynamicKey, CorfuDynamicRecord> genericCachedTable = new ConcurrentHashMap<>();

        for (File file : files) {
            try (FileChannel fileChannel = FileChannel.open(file.toPath())) {
                // set the file channel's position back to 0
                fileChannel.position(0);
                //long pos = fileChannel.size();

                // parse header
                LogFormat.LogHeader header = parseHeader(null, fileChannel, file.getAbsolutePath());
                //System.out.println(header);

                // iterate through the file
                // make sure that fileChannel.size() - fileChannel.position() > 14 to prevent
                // actualMetaDataSize < METADATA_SIZE, which would create an exception
                while (fileChannel.size() - fileChannel.position() > 14) {
                    //long channelOffset = fileChannel.position();
                    //System.out.println(channelOffset);

                    // parse metadata and entry
                    LogFormat.Metadata metadata = StreamLogFiles.parseMetadata(null, fileChannel, file.getAbsolutePath());
                    LogEntry entry = StreamLogFiles.parseEntry(null, fileChannel, metadata, file.getAbsolutePath());
                    //System.out.println(metadata);
                    //System.out.println(entry);

                    if(metadata != null && entry != null) {
                        // convert the LogEntry to LogData to access getPayload
                        LogData data = StreamLogFiles.getLogData(entry);
                        //System.out.println(data.getData());

                        processGenericLogData(data, genericTableStreamId, genericTableCheckpointStream, runtimeWithOnlyProtoSerializer, genericCachedTable, genericTableEntries);
                    }

                }
                System.out.println("Finished processing file: " + file.getAbsolutePath());

            } catch (IOException e) {
                throw new IllegalStateException("Invalid header: " + file.getAbsolutePath(), e);
            }
        }

        return genericCachedTable.size();
    }

    /**
     * List all tables in CorfuStore
     * @param namespace - the namespace where the table belongs
     * @return - number of tables in this namespace
     */
    @Override
    public int listTables(String namespace)
    {
        int numTables = 0;
        System.out.println("\n=====Tables=======\n");
        for (CorfuStoreMetadata.TableName tableName : listTablesInNamespace(namespace)) {
            System.out.println("Table: " + tableName.getTableName());
            System.out.println("Namespace: " + tableName.getNamespace());
            numTables++;
        }
        System.out.println("\n======================\n");
        return numTables;
    }

    public List<CorfuStoreMetadata.TableName> listTablesInNamespace(String namespace) {
        return dynamicProtobufSerializer.getCachedRegistryTable().keySet()
                .stream()
                .filter(tableName -> namespace == null || tableName.getNamespace().equals(namespace))
                .collect(Collectors.toList());
    }

    /**
     * Print information about a specific table in CorfuStore
     * @param namespace - the namespace where the table belongs
     * @param tablename - table name without the namespace
     * @return - number of entries in the table
     */
    @Override
    public int printTableInfo(String namespace, String tablename) {
        System.out.println("\n======================\n");
        String fullName = TableRegistry.getFullyQualifiedTableName(namespace, tablename);
        UUID streamUUID = UUID.nameUUIDFromBytes(fullName.getBytes());
        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table =
                getTable(namespace, tablename);
        int tableSize = table.size();
        System.out.println("Table " + tablename + " in namespace " + namespace +
                " with ID " + streamUUID.toString() + " has " + tableSize + " entries");
        System.out.println("\n======================\n");
        return tableSize;
    }

    /**
     * Helper to analyze all the protobufs used in this cluster
     */
    @Override
    public int printAllProtoDescriptors() {
        int numProtoFiles = -1;
        System.out.println("=========PROTOBUF FILE NAMES===========");
        for (CorfuStoreMetadata.ProtobufFileName protoFileName :
                dynamicProtobufSerializer.getCachedProtobufDescriptorTable().keySet()) {
            try {
                System.out.println(JsonFormat.printer().print(protoFileName));
            } catch (InvalidProtocolBufferException e) {
                System.out.println("Unable to print protobuf for key " + protoFileName + e);
                //log.error("Unable to print protobuf for key {}", protoFileName, e);
            }
            numProtoFiles++;
        }
        System.out.println("=========PROTOBUF FILE DESCRIPTORS ===========");
        for (CorfuStoreMetadata.ProtobufFileName protoFileName :
                dynamicProtobufSerializer.getCachedProtobufDescriptorTable().keySet()) {
            try {
                System.out.println(JsonFormat.printer().print(protoFileName));
                System.out.println(JsonFormat.printer().print(
                        dynamicProtobufSerializer.getCachedProtobufDescriptorTable()
                                .get(protoFileName).getPayload())
                );
            } catch (InvalidProtocolBufferException e) {
                System.out.println("Unable to print protobuf for key " + protoFileName + e);
                //log.error("Unable to print protobuf for key {}", protoFileName, e);
            }
        }
        return numProtoFiles;
    }

    @Override
    public int clearTable(String namespace, String tablename) {
        return -1;
    }

    @Override
    public CorfuDynamicRecord addRecord(String namespace, String tableName, String newKey, String newValue, String newMetadata) {
        return null;
    }

    @Override
    public CorfuDynamicRecord editRecord(String namespace, String tableName, String keyToEdit, String newRecord) {
        return null;
    }

    @Override
    public int deleteRecordsFromFile(String namespace, String tableName, String pathToKeysFile, int batchSize) {
        return 0;
    }

    @Override
    public int deleteRecords(String namespace, String tableName, List<String> keysToDelete, int batchSize) {
        return 0;
    }

    @Override
    public int loadTable(String namespace, String tableName, int numItems, int batchSize, int itemSize) {
        return 0;
    }

    @Override
    public int listenOnTable(String namespace, String tableName, int stopAfter) {
        return 0;
    }

    @Override
    public Set<String> listStreamTags() {
        return null;
    }

    @Override
    public Map<String, List<CorfuStoreMetadata.TableName>> listTagToTableMap() {
        return null;
    }

    @Override
    public Set<String> listTagsForTable(String namespace, String table) {
        return null;
    }

    @Override
    public List<CorfuStoreMetadata.TableName> listTablesForTag(@Nonnull String streamTag) {
        return null;
    }
}

/**
 * Wrapper class for LogEntry objects with an address
 */
class LogEntryOrdering {
    org.corfudb.protocols.logprotocol.LogEntry obj;
    long ordering; //address is stored here

    public LogEntryOrdering(org.corfudb.protocols.logprotocol.LogEntry obj, long ordering) {
        this.obj = obj;
        this.ordering = ordering;
    }

    public org.corfudb.protocols.logprotocol.LogEntry getObj() {
        return obj;
    }

    public void setObj(org.corfudb.protocols.logprotocol.LogEntry obj) {
        this.obj = obj;
    }

    public long getOrdering() {
        return ordering;
    }

    public void setOrdering(long ordering) {
        this.ordering = ordering;
    }
}

/**
 * Comparator class for LogEntry objects
 */
class LogEntryComparator implements Comparator<LogEntryOrdering> {
    @Override
    public int compare(LogEntryOrdering a, LogEntryOrdering b) {
        return Long.compare(a.getOrdering(), b.getOrdering());
    }
}