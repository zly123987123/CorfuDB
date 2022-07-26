package org.corfudb.runtime.object;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * MVOCache is the centralized container that holds the reference to
 * all MVOs. Overall it provides put and get APIs and manages the
 * Cache-related properties (LRU) under the hood.
 *
 */
@Slf4j
public class MVOCache<T extends ICorfuSMR<T>> {

    private final CorfuRuntime runtime;

    // A registry to keep track of all opened MVOs.
    @Getter
    private final ConcurrentHashMap<UUID, MultiVersionObject<T>> allMVOs = new ConcurrentHashMap<>();

    // A collection of strong references to all versioned objects and their snapshotProxies.
    private final Cache<VersionedObjectIdentifier, MVOCacheEntry> objectCache;

    // An ordered view of all versions grouped by each MVO's UUID. Used to facilitate operations such as floorEntry.
    // Modifications to the same MVO should be explicitly synchronized.
    @Getter
    private final ConcurrentHashMap<UUID, TreeSet<Long>> objectVersions = new ConcurrentHashMap<>();

    /**
     * TODO: access pattern
     * Mutations: 1) objectCache, 2) objectVersions
     * Accesses/Gets: 1) objectVersions, 2) query objectCache for real object if previous query is true
     */

    private static final long DEFAULT_CACHE_EXPIRY_TIME_IN_SECONDS = 300;

    private final ScheduledExecutorService mvoCacheSyncThread = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("MVOCacheSyncThread")
                    .build());

    @Getter
    private final MVOCacheEviction mvoCacheEviction = new MVOCacheEviction(this::prefixEvict);

    public MVOCache(CorfuRuntime corfuRuntime) {
        runtime = corfuRuntime;

        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        objectCache = cacheBuilder.maximumSize(runtime.getParameters().getMaxCacheEntries())
                .expireAfterAccess(DEFAULT_CACHE_EXPIRY_TIME_IN_SECONDS, TimeUnit.SECONDS)
                .expireAfterWrite(DEFAULT_CACHE_EXPIRY_TIME_IN_SECONDS, TimeUnit.SECONDS)
                .removalListener(this::handleEviction)
                .recordStats()
                .build();

        mvoCacheSyncThread.scheduleAtFixedRate(this::syncMVOCache,
                runtime.getParameters().getMvoAutoSyncPeriod().toMillis(),
                runtime.getParameters().getMvoAutoSyncPeriod().toMillis(),
                TimeUnit.MILLISECONDS);

        mvoCacheEviction.start();
    }

    public void shutdown() {
        mvoCacheSyncThread.shutdownNow();
        mvoCacheEviction.shutdown();
    }

    /**
     * Callback function which is triggered by every guava cache eviction.
     *
     * @param notification guava cache eviction notification
     */
    private void handleEviction(RemovalNotification<VersionedObjectIdentifier, Object> notification) {
        if (notification.getCause() == RemovalCause.EXPLICIT ||
                notification.getCause() == RemovalCause.REPLACED)
            return;
        mvoCacheEviction.add(notification.getKey());
    }

    /**
     * Evict all versions of the given object up to the target version.
     *
     * @param voId  the object and version to perform prefixEvict
     * @return number of versions that has been evicted
     */
    public int prefixEvict(VersionedObjectIdentifier voId) {
        TreeSet<Long> allVersionsOfThisObject = objectVersions.get(voId.getObjectId());
        if (allVersionsOfThisObject == null) {
            return 0;
        }
        int count;

        synchronized (allVersionsOfThisObject) {
            // set 'inclusive' to make objectCache and objectVersions consistent
            Set<Long> headSet = allVersionsOfThisObject.headSet(voId.getVersion(), true);
            headSet.forEach(version -> {
                voId.setVersion(version);
                objectCache.invalidate(voId);
            });
            count = headSet.size();
            headSet.clear();
        }

        return count;
    }

    /**
     * Get the Snapshot Proxy of the versioned object specified by the given voId
     * generated by the given snapshot proxy generator. Result is wrapped by Optional.
     * Return Optional.empty() if the MVO cache does not have the versioned object
     * specified by the voId.
     *
     * @param voId the id of the versioned object
     * @param snapshotProxyFn the snapshot proxy generator
     * @return an Optional of the versioned object. Empty if not present in cache.
     */
    public Optional<ICorfuSMRSnapshotProxy<T>> get(@NonNull VersionedObjectIdentifier voId,
                                                   @NonNull ISnapshotProxyGenerator<T> snapshotProxyFn) {
        MVOCacheEntry cacheEntry = objectCache.getIfPresent(voId);
        if (cacheEntry == null) {
            return Optional.empty();
        }

        final ICorfuSMRSnapshotProxy<T> snapshotProxy = snapshotProxyFn.generate(voId, cacheEntry.getBaseSnapshot());
        if (snapshotProxy instanceof SnapshotReferenceProxy) {
            cacheEntry.getSnapshotProxies().add(snapshotProxy);
        }
        return Optional.of(snapshotProxy);
    }

    /**
     * Put the voId and versionedObject to objectCache, update objectVersions.
     *
     * @param voId the object and version to add to cache
     * @param versionedObject the versioned object to add
     */
    public void put(@NonNull VersionedObjectIdentifier voId, @NonNull T versionedObject) {
        TreeSet<Long> allVersionsOfThisObject = objectVersions.computeIfAbsent(
                voId.getObjectId(), k -> new TreeSet<>());
        synchronized (allVersionsOfThisObject) {
            // TODO: What if voId is already there?
            objectCache.put(voId, new MVOCacheEntry(versionedObject));
            allVersionsOfThisObject.add(voId.getVersion());
        }
    }

    /**
     * Returns true if the objectCache has the target voId.
     *
     * @param voId the object and version to check
     * @return true if target exists
     */
    public Boolean containsKey(VersionedObjectIdentifier voId) {
        return objectCache.asMap().containsKey(voId);
    }

    /**
     * For a given voId, find in all versions of this object that has the greatest
     * version && version <= voId.getVersion.
     *
     * @param voId the object and version to check
     * @param snapshotProxyFn snapshotProxy generator
     * @return an optional of an ICorfuSMRSnapshotProxy instance which is generated
     *         by the given snapshotProxy generator function. Empty if there is no floorEntry.
     */
    public Optional<ICorfuSMRSnapshotProxy<T>> floorEntry(@NonNull VersionedObjectIdentifier voId,
                                                          @NonNull ISnapshotProxyGenerator<T> snapshotProxyFn) {
        final TreeSet<Long> allVersionsOfThisObject = objectVersions.get(voId.getObjectId());

        if (allVersionsOfThisObject == null) {
            // The object has not been created
            return Optional.empty();
        } else {
            synchronized (allVersionsOfThisObject) {
                final Long floorVersion = allVersionsOfThisObject.floor(voId.getVersion());
                if (floorVersion == null) {
                    return Optional.empty();
                }

                final VersionedObjectIdentifier id = new VersionedObjectIdentifier(voId.getObjectId(), floorVersion);
                final MVOCacheEntry cacheEntry = objectCache.getIfPresent(id);
                // Cache eviction updates objectVersions asynchronously.
                // The item is removed from objectVersions after cache eviction happens.
                if (cacheEntry == null) {
                    return Optional.empty();
                }

                final ICorfuSMRSnapshotProxy<T> snapshotProxy = snapshotProxyFn.generate(id, cacheEntry.getBaseSnapshot());
                cacheEntry.getSnapshotProxies().add(snapshotProxy);
                return Optional.of(snapshotProxy);
            }
        }
    }

    /**
     * Returns true if objectCache has any versionedObject of a certain object.
     * Checks the objectVersions.
     *
     * @param objectId the object id to check
     * @return true if target object exists
     */
    public Boolean containsObject(UUID objectId) {
        // TODO: what if an object existing in objectCache but its version is not in objectVersions?
        // This can happen when MVOCache::put is interrupted
        return objectVersions.containsKey(objectId);
    }

    public void registerMVO(UUID objectId, MultiVersionObject<T> mvo) {
        allMVOs.computeIfAbsent(objectId, key -> mvo);
    }

    private void syncMVOCache() {
        TokenResponse streamTails = runtime.getSequencerView()
                .query(allMVOs.keySet().toArray(new UUID[0]));
        final AtomicInteger count = new AtomicInteger(0);
        allMVOs.forEach((uuid, mvo) -> {
            if (objectVersions.get(uuid) != null &&
                    objectVersions.get(uuid).last() < streamTails.getStreamTail(uuid)) {
                // Sync to the latest state
                mvo.getVersionedObjectUnderLock(streamTails.getStreamTail(uuid), v -> {});
                count.getAndIncrement();
            }
        });
        log.info("Synced {} objects.", count);
    }

    // For testing purpose
    public Set<VersionedObjectIdentifier> keySet() {
        return ImmutableSet.copyOf(objectCache.asMap().keySet());
    }

    private class MVOCacheEntry {
        @Getter
        private final T baseSnapshot;

        // This is used to "tie" the snapshot proxies given out with the immutable state in the cache.
        // Snapshot proxies should not be invalidated while the cache maintains this entry.
        @Getter
        private final Collection<ICorfuSMRSnapshotProxy<T>> snapshotProxies;

        MVOCacheEntry(@NonNull final T baseSnapshot) {
            this.baseSnapshot = baseSnapshot;

            // ConcurrentLinkedQueue provides fast insertions in some ordering
            this.snapshotProxies = new ConcurrentLinkedQueue<>();
        }
    }
}
