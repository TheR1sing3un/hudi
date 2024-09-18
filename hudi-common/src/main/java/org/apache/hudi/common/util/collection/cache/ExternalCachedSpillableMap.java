package org.apache.hudi.common.util.collection.cache;

import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.AbstractExternalSpillableMap;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;

import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.Serializable;

public class ExternalCachedSpillableMap<K extends Serializable, R extends Serializable> extends AbstractExternalSpillableMap<K, R> {

  private final Cache<>

  public ExternalCachedSpillableMap(long maxInMemorySizeInBytes, String baseFilePath, SizeEstimator keySizeEstimator,
                                    SizeEstimator valueSizeEstimator) throws IOException {
    super(maxInMemorySizeInBytes, baseFilePath, keySizeEstimator, valueSizeEstimator);
  }

  public ExternalCachedSpillableMap(long maxInMemorySizeInBytes, String baseFilePath, SizeEstimator keySizeEstimator, SizeEstimator valueSizeEstimator, DiskMapType diskMapType) throws IOException {
    super(maxInMemorySizeInBytes, baseFilePath, keySizeEstimator, valueSizeEstimator, diskMapType);
  }

  public ExternalCachedSpillableMap(long maxInMemorySizeInBytes, String baseFilePath, SizeEstimator keySizeEstimator, SizeEstimator valueSizeEstimator, DiskMapType diskMapType,
                                    boolean isCompressionEnabled) throws IOException {
    super(maxInMemorySizeInBytes, baseFilePath, keySizeEstimator, valueSizeEstimator, diskMapType, isCompressionEnabled);
  }

  @Override
  public Object get(Object key) {
    return null;
  }

  @Nullable
  @Override
  public Object put(Object key, Object value) {
    return null;
  }
}
