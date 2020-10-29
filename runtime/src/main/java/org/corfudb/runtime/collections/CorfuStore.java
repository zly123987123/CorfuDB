package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;

/**
 * CorfuStore is a protobuf API layer that provides all the features of CorfuDB.
 *
 * Key APIs exposed are:
 *   o-> TxnContext() for CRUD operations
 *   o-> getTimestamp() for database snapshots
 *   o-> table lifecycle management
 *
 * Created by zlokhandwala on 2019-08-02.
 */
public class CorfuStore {

    @Getter
    private final CorfuRuntime runtime;

    /**
     * Creates a new CorfuStore.
     *
     * @param runtime Connected instance of the Corfu Runtime.
     */
    @Nonnull
    public CorfuStore(@Nonnull final CorfuRuntime runtime) {
        this(runtime, true);
    }

    /**
     * Creates a new CorfuStore.
     *
     * @param runtime Connected instance of the Corfu Runtime.
     * @param enableTxLogging
     */
    @Nonnull
    public CorfuStore(@Nonnull final CorfuRuntime runtime, boolean enableTxLogging) {
        runtime.setTransactionLogging(enableTxLogging);
        this.runtime = runtime;
    }

    /**
     * Fetches the latest logical timestamp (global tail) in Corfu's distributed log.
     *
     * @return Timestamp.
     */
    @Nonnull
    public Timestamp getTimestamp() {
        Token token = runtime.getSequencerView().query().getToken();
        return Timestamp.newBuilder()
                .setEpoch(token.getEpoch())
                .setSequence(token.getSequence())
                .build();
    }

    /**
     * Creates and registers a table.
     * A table needs to be registered before it is used.
     *
     * @param namespace    Namespace of the table.
     * @param tableName    Table name.
     * @param kClass       Class of the Key Model.
     * @param vClass       Class of the Value Model.
     * @param mClass       Class of the Metadata Model.
     * @param tableOptions Table options.
     * @param <K>          Key type.
     * @param <V>          Value type.
     * @param <M>          Type of Metadata.
     * @return Table instance.
     * @throws NoSuchMethodException     Thrown if key/value class are not protobuf classes.
     * @throws InvocationTargetException Thrown if key/value class are not protobuf classes.
     * @throws IllegalAccessException    Thrown if key/value class are not protobuf classes.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> openTable(@Nonnull final String namespace,
                             @Nonnull final String tableName,
                             @Nonnull final Class<K> kClass,
                             @Nonnull final Class<V> vClass,
                             @Nullable final Class<M> mClass,
                             @Nonnull final TableOptions tableOptions)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        return runtime.getTableRegistry().openTable(namespace, tableName, kClass, vClass, mClass, tableOptions);
    }

    /**
     * Fetches an existing table. This table should have been registered with this instance
     * of the Corfu runtime.
     *
     * @param namespace Namespace of the table.
     * @param tableName Table name.
     * @return Table instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTable(@Nonnull final String namespace,
                            @Nonnull final String tableName) {
        return runtime.getTableRegistry().getTable(namespace, tableName);
    }

    /**
     * Deletes a table instance. [NOT SUPPORTED.]
     *
     * @param namespace Namespace of the table.
     * @param tableName Table name.
     */
    public void deleteTable(String namespace, String tableName) {
        runtime.getTableRegistry().deleteTable(namespace, tableName);
    }

    /**
     * Lists all the tables in a particular namespace.
     * Lists all the tables in the database if namespace is null.
     *
     * @param namespace Namespace to query.
     * @return Collection of TableNames.
     */
    @Nonnull
    public Collection<TableName> listTables(@Nullable final String namespace) {
        return runtime.getTableRegistry().listTables(namespace);
    }

    /**
     * Start a transaction with snapshot isolation level at the latest available corfu snapshot.
     * The transaction does not begin until either a commit is invoked or a read happens.
     *
     * @param namespace Namespace of the tables involved in the transaction.
     * @return Returns a transaction builder instance.
     */
    @Nonnull
    @Deprecated
    public TxBuilder tx(@Nonnull final String namespace) {
        return new TxBuilder(
                this.runtime.getObjectsView(),
                this.runtime.getTableRegistry(),
                namespace);
    }

    /**
     * Start a transaction with snapshot isolation level at the latest available snapshot.
     *
     * @param namespace Namespace of the tables involved in the transaction.
     * @return Returns a Transaction context.
     */
    @Nonnull
    public TxnContext txn(@Nonnull final String namespace) {
        return new TxnContext(
                this.runtime.getObjectsView(),
                this.runtime.getTableRegistry(),
                namespace,
                IsolationLevel.snapshot());
    }

    /**
     * Start appending mutations to a transaction.
     * The transaction does not begin until either a commit or the first read is invoked.
     * On read or commit the latest available snapshot will be used to resolve the transaction
     * unless the isolation level has a snapshot timestamp value specified.
     *
     * @param namespace      Namespace of the tables involved in the transaction.
     * @param isolationLevel Snapshot (latest or specific) at which the transaction must execute.
     * @return Returns a transaction context instance.
     */
    @Nonnull
    public TxnContext txn(@Nonnull final String namespace, IsolationLevel isolationLevel) {
        return new TxnContext(
                this.runtime.getObjectsView(),
                this.runtime.getTableRegistry(),
                namespace,
                isolationLevel);
    }

    /**
     * Provides a query interface.
     *
     * @param namespace Namespace within which the queries are executed.
     * @return Query implementation.
     */
    @Nonnull
    @Deprecated
    public Query query(@Nonnull final String namespace) {
        return new Query(
                this.runtime.getTableRegistry(),
                this.runtime.getObjectsView(),
                namespace);
    }

    /**
     * Subscribe to transaction updates on specific tables in the namespace.
     * Objects returned will honor transactional boundaries.
     *
     * @param streamListener   callback context
     * @param namespace        the CorfuStore namespace to subscribe to
     * @param tablesOfInterest only updates from these tables of interest will be sent to listener
     * @param timestamp        if specified, all stream updates from this timestamp will be returned
     *                         if null, only future updates will be returned
     */
    @Deprecated
    public <K extends Message, V extends Message, M extends Message>
    void subscribe(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                   @Nonnull List<TableSchema<K, V, M>> tablesOfInterest,
                   @Nullable Timestamp timestamp) {
        runtime.getTableRegistry().getStreamManager()
                .subscribe(streamListener, namespace, tablesOfInterest,
                        (timestamp == null) ? getTimestamp().getSequence() : timestamp.getSequence());
    }

    /**
     * Subscribe to transaction updates on specific tables with the streamTag in the namespace.
     * Objects returned will honor transactional boundaries.
     *
     * @param streamListener   callback context
     * @param namespace        the CorfuStore namespace to subscribe to
     * @param streamTag        only updates of tables with the stream tag will be polled
     * @param tablesOfInterest only updates from these tables of interest will be sent to listener
     * @param timestamp        if specified, all stream updates from this timestamp will be returned
     *                         if null, only future updates will be returned
     */
    public void subscribe(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                   @Nonnull String streamTag, @Nonnull List<String> tablesOfInterest,
                   @Nullable Timestamp timestamp) {
        runtime.getTableRegistry().getStreamManager()
                .subscribe(streamListener, namespace, streamTag, tablesOfInterest,
                        (timestamp == null) ? getTimestamp().getSequence() : timestamp.getSequence());
    }

    /**
     * Gracefully shutdown a streamer.
     * Once this call returns no further stream updates will be returned.
     *
     * @param streamListener - callback context.
     */
    public void unsubscribe(@Nonnull StreamListener streamListener) {
        runtime.getTableRegistry().getStreamManager()
                .unsubscribe(streamListener);
    }
}
