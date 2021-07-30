package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.view.Address;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Sequencer server cache.
 * Contains transaction conflict-resolution data structures.
 * <p>
 * The conflictKeyMap maps conflict keys (stream id + key) to versions (long), illustrated below:
 * Conflict Key | ck1 | ck2 | ck3 | ck4
 * Version | v1 | v1 | v2 | v3
 * Consider the case where we need to insert a new conflict key (ck), but the cache is full.
 * We need to evict the oldest conflict keys. In the example above, we can't just evict ck1,
 * we need to evict all keys that map to v1, so we need to evict ck1 and ck2. This eviction
 * policy is FIFO on the version number. Notice that we also can't evict ck3 before the keys
 * for v1, that's because it will create holes in the resolution window and can lead to
 * incorrect resolutions.
 * <p>
 * We use TreeMap as a sliding window on the versions, where a version can map to multiple keys,
 * so we also need to maintain the beginning of the window which is the maxConflictWildcard variable.
 * <p>
 * SequencerServerCache achieves consistency by using single threaded cache.
 */
@NotThreadSafe
@Slf4j
public class SequencerServerCache implements AutoCloseable {

    /**
     * A mapping between conflict keys and their latest global-log position.
     */
    @VisibleForTesting
    @Getter(AccessLevel.PUBLIC)
    private final Map<ConflictTxStream, Long> conflictKeyMap;

    /**
     * A mapping between global-log positions and conflict keys. This is maintained to
     * facilitate the eviction of all conflict keys associated with a particular transaction
     * commit timestamp.
     */
    @VisibleForTesting
    @Getter(AccessLevel.PUBLIC)
    private final SortedMap<Long, Set<ConflictTxStream>> versionMap;

    /**
     * The max number of entries that the SequencerServerCache may contain.
     */
    @Getter
    private final int capacity;

    /**
     * A "wildcard" representing the maximal update timestamp of
     * all the conflict keys which were evicted from the cache
     */
    @Getter
    private long maxConflictWildcard;

    /**
     * maxConflictNewSequencer represents the max update timestamp of all the conflict keys
     * which were evicted from the cache by the time this server is elected
     * the primary sequencer. This means that any snapshot timestamp below this
     * actual threshold would abort due to NEW_SEQUENCER cause.
     */
    @Getter
    private final long maxConflictNewSequencer;

    private static final String CONFLICT_KEYS_COUNTER_NAME = "sequencer.conflict-keys.size";

    private static final String WINDOW_SIZE_NAME = "sequencer.cache.window";

    /**
     * The Sequencers conflict key cache, limited by size.
     * @param capacity                The max capacity of the cache.
     * @param maxConflictNewSequencer The new max update timestamp of all conflict keys evicted
     *                                from the cache by the time this server is elected as
     *                                primary sequencer.
     */
    public SequencerServerCache(int capacity, long maxConflictNewSequencer) {
        Preconditions.checkArgument(capacity > 0, "sequencer cache capacity must be positive.");

        this.capacity = capacity;
        this.maxConflictWildcard = maxConflictNewSequencer;
        this.maxConflictNewSequencer = maxConflictNewSequencer;

        this.conflictKeyMap = MicroMeterUtils
                .gauge(CONFLICT_KEYS_COUNTER_NAME, new HashMap<ConflictTxStream, Long>(), HashMap::size)
                .orElseGet(HashMap::new);

        this.versionMap = MicroMeterUtils
                .gauge(WINDOW_SIZE_NAME, new TreeMap<Long, Set<ConflictTxStream>>(), TreeMap::size)
                .orElseGet(TreeMap::new);
    }

    /**
     * Returns the global address associated with the conflict key in this cache,
     * or {@code Address.NON_ADDRESS} if there is no such cached address.
     * @param conflictKey The conflict key.
     * @return The global address associated with this hashed conflict key, if present.
     */
    public long get(@NonNull ConflictTxStream conflictKey) {
        return conflictKeyMap.getOrDefault(conflictKey, Address.NON_ADDRESS);
    }

    /**
     * Returns the first (smallest) address in the cache.
     * @return The smallest address currently in the cache.
     */
    public long firstAddress() {
        if (versionMap.isEmpty()) {
            return Address.NOT_FOUND;
        }

        return versionMap.firstKey();
    }

    /**
     * Evict the records with the smallest address in the cache.
     * It could be one or multiple records.
     *
     * @return The number of entries that have been evicted from the cache.
     */
    private int evictSmallestTxVersion() {
      final long firstAddress = firstAddress();
      final Set<ConflictTxStream> entriesToDelete = versionMap.remove(firstAddress);
      final int numDeletedEntries = entriesToDelete.size();
      log.trace("evictSmallestTxVersion: items evicted {} min address {}", numDeletedEntries, firstAddress);
      entriesToDelete.forEach(conflictKeyMap::remove);
      maxConflictWildcard = Math.max(maxConflictWildcard, firstAddress);
      return numDeletedEntries;
    }

    /**
     * Evict records from the cache until the number of records is
     * equal to or less than the cache capacity. Eviction is performed
     * by repeatedly evicting all of the records with the smallest address.
     */
    private void evict() {
        while (conflictKeyMap.size() > capacity) {
            evictSmallestTxVersion();
        }
    }

    /**
     * Evict all records up to a trim mark (not included).
     * @param trimMark The trim mark.
     */
    public void evictUpTo(long trimMark) {
        final SortedMap<Long, Set<ConflictTxStream>> entriesToEvict = versionMap.headMap(trimMark);
        final long numTxVersionsToEvict = entriesToEvict.size();
        long numConflictKeysEvicted = 0;

        if (numTxVersionsToEvict > 0) {
            log.debug("evictUpTo: trim mark {}", trimMark);
            for (int l = 0; l < numTxVersionsToEvict; l++) {
                numConflictKeysEvicted += evictSmallestTxVersion();
            }
        }

        log.info("evictUpTo: entries={} addresses={}", numConflictKeysEvicted, numTxVersionsToEvict);
        MicroMeterUtils.measure(numConflictKeysEvicted, "sequencer.cache.evictions");
    }

    /**
     * The cache size as the number of entries.
     * @return The current cache size.
     */
    public int size() {
        return conflictKeyMap.size();
    }

    /**
     * Put the provided hashed conflict keys into the cache,
     * and evict older entries as necessary.
     * @param conflictKeys The conflict keys to add into the cache.
     * @param txVersion    The timestamp associated with these conflict keys.
     */
    public void put(@NonNull Set<ConflictTxStream> conflictKeys, long txVersion) {
        // The provided timestamp should not already be present in the cache.
        Preconditions.checkState(!versionMap.containsKey(txVersion),
                "txVersion=%s is already present in SequencerServerCache", txVersion);

        // The provided timestamp should be greater than the smallest address in the cache.
        final long smallestAddress = firstAddress();
        Preconditions.checkArgument(txVersion > smallestAddress,
                "txVersion=%s should be larger than the smallest address=%s", txVersion, smallestAddress);

        // The provided timestamp should be greater than any previous timestamp associated provided conflict
        // key information. This check is performed separately to avoid partially updating the cache.
        conflictKeys.parallelStream().forEach(conflictKey -> {
            final long prevVersion = get(conflictKey);
            Preconditions.checkState(prevVersion < txVersion,
                    "sequencer regression: conflictKey=%s prevVersion=%s txVersion=%s",
                    conflictKey, prevVersion, txVersion);
        });

        conflictKeys.forEach(conflictKey -> {
            final Long prevVersion = conflictKeyMap.put(conflictKey, txVersion);

            // Remove the previous entry if conflict key was present with an older version
            if (prevVersion != null) {
                final Set<ConflictTxStream> entries = versionMap.get(prevVersion);
                entries.remove(conflictKey);
                if (entries.isEmpty()) {
                    versionMap.remove(prevVersion);
                }
            }
        });

        versionMap.put(txVersion, conflictKeys);

        // If applicable, evict entries until we are below the max cache capacity.
        // Note: this can trigger the eviction of multiple conflict keys from multiple timestamps.
        evict();
    }

    @Override
    public void close() {
        MicroMeterUtils.removeGaugesWithNoTags(WINDOW_SIZE_NAME, CONFLICT_KEYS_COUNTER_NAME);
    }

    /**
     * Contains the conflict hash code for a stream ID and conflict param.
     */
    @EqualsAndHashCode
    public static class ConflictTxStream {

        @Getter
        private final UUID streamId;

        @Getter
        private final byte[] conflictParam;

        public ConflictTxStream(UUID streamId, byte[] conflictParam) {
            this.streamId = streamId;
            this.conflictParam = conflictParam;
        }

        @Override
        public String toString() {
            return streamId.toString() + Arrays.toString(conflictParam);
        }
    }
}
