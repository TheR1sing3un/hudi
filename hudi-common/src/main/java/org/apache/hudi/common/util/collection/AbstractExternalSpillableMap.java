package org.apache.hudi.common.util.collection;

import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.exception.HoodieIOException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public abstract class AbstractExternalSpillableMap<T extends Serializable, R extends Serializable> implements Map<T, R>, Serializable, Closeable, Iterable<R> {
  // Find the actual estimated payload size after inserting N records
  private static final int NUMBER_OF_RECORDS_TO_ESTIMATE_PAYLOAD_SIZE = 100;
  // Map to store key-values on disk or db after it spilled over the memory
  private transient volatile DiskMap<T , R> diskBasedMap;
  // TODO(na) : a dynamic sizing factor to ensure we have space for other objects in memory and
  // incorrect payload estimation
  private static final double SIZING_FACTOR_FOR_IN_MEMORY_MAP = 0.8;
  // Size Estimator for key type
  private final SizeEstimator<T> keySizeEstimator;
  // Size Estimator for key types
  private final SizeEstimator<R> valueSizeEstimator;
  // Type of the disk map
  private final ExternalSpillableMap.DiskMapType diskMapType;
  // Enables compression of values stored in disc
  private final boolean isCompressionEnabled;
  // Base File Path
  private final String baseFilePath;

  public AbstractExternalSpillableMap(long maxInMemorySizeInBytes, String baseFilePath, SizeEstimator<T> keySizeEstimator,
                              SizeEstimator<R> valueSizeEstimator) throws IOException {
    this(maxInMemorySizeInBytes, baseFilePath, keySizeEstimator, valueSizeEstimator, ExternalSpillableMap.DiskMapType.BITCASK);
  }

  public AbstractExternalSpillableMap(long maxInMemorySizeInBytes, String baseFilePath, SizeEstimator<T> keySizeEstimator,
                              SizeEstimator<R> valueSizeEstimator, ExternalSpillableMap.DiskMapType diskMapType) throws IOException {
    this(maxInMemorySizeInBytes, baseFilePath, keySizeEstimator, valueSizeEstimator, diskMapType, false);
  }

  public AbstractExternalSpillableMap(long maxInMemorySizeInBytes, String baseFilePath, SizeEstimator<T> keySizeEstimator,
                              SizeEstimator<R> valueSizeEstimator, ExternalSpillableMap.DiskMapType diskMapType, boolean isCompressionEnabled) throws IOException {
    this.baseFilePath = baseFilePath;
    this.keySizeEstimator = keySizeEstimator;
    this.valueSizeEstimator = valueSizeEstimator;
    this.diskMapType = diskMapType;
    this.isCompressionEnabled = isCompressionEnabled;
  }

  private void initDiskBasedMap() {
    if (null == diskBasedMap) {
      synchronized (this) {
        if (null == diskBasedMap) {
          try {
            switch (diskMapType) {
              case ROCKS_DB:
                diskBasedMap = new RocksDbDiskMap<>(baseFilePath);
                break;
              case BITCASK:
              default:
                diskBasedMap = new BitCaskDiskMap<>(baseFilePath, isCompressionEnabled);
            }
          } catch (IOException e) {
            throw new HoodieIOException(e.getMessage(), e);
          }
        }
      }
    }
  }

  @Override
  public void clear() {
    if (diskBasedMap != null) {
      diskBasedMap.clear();
    }
  }

  @Override
  public void close() throws IOException {
    if (diskBasedMap != null) {
      diskBasedMap.close();
    }
  }

  @NotNull
  @Override
  public Set<T> keySet() {
    return diskBasedMap != null ? diskBasedMap.keySet() : Collections.emptySet();
  }

  @NotNull
  @Override
  public Collection<R> values() {
    return diskBasedMap != null ? diskBasedMap.values() : Collections.emptyList();
  }

  public Stream<R> valueStream() {
    return diskBasedMap != null ? diskBasedMap.valueStream() : Stream.empty();
  }

  @NotNull
  @Override
  public Set<Entry<T, R>> entrySet() {
    return diskBasedMap != null ? diskBasedMap.entrySet() : Collections.emptySet();
  }


  /**
   * The type of map to use for storing the Key, values on disk after it spills
   * from memory in the {@link ExternalSpillableMap}.
   */
  public enum DiskMapType {
    BITCASK,
    ROCKS_DB,
    UNKNOWN
  }

  public int getExternalMapNumEntries() {
    return diskBasedMap != null ? diskBasedMap.size() : 0;
  }

  public long getExternalMapSizeInBytes() {
    return diskBasedMap != null ? diskBasedMap.sizeOfFileOnDiskInBytes() : 0;
  }

  @Override
  public int size() {
    return getExternalMapNumEntries();
  }

  @Override
  public boolean isEmpty() {
    return getExternalMapNumEntries() == 0;
  }

  @Override
  public boolean containsKey(Object key) {
    return diskBasedMap != null && diskBasedMap.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return diskBasedMap != null && diskBasedMap.containsValue(value);
  }

  @Nullable
  @Override
  public R put(T key, R value) {
    if (diskBasedMap == null) {
      initDiskBasedMap();
    }
    return diskBasedMap.put(key, value);
  }

  @Override
  public R remove(Object key) {
    return diskBasedMap != null ? diskBasedMap.remove(key) : null;
  }

  @Override
  public void putAll(@NotNull Map<? extends T, ? extends R> m) {
    for (Entry<? extends T, ? extends R> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @NotNull
  @Override
  public Iterator<R> iterator() {
    return valueStream().iterator();
  }
}
