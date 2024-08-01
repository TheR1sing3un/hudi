package org.apache.hudi.common.util.collection.cache;

import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;

import java.io.IOException;

public class ExternalCachedSpillableMap extends ExternalSpillableMap {



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
}
