package org.apache.hudi.io;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.BaseHoodieRecord;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class HoodieUnmergedCreateHandle<T, I, K, O> extends HoodieCreateHandle<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieUnmergedCreateHandle.class);

  private Iterator<BaseHoodieRecord> unmergedRecordsIter;

  private Option<HoodieRecord<T>> currentMergedRecord = Option.empty();

  private boolean currentMergedRecordMerged = false;

  public HoodieUnmergedCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable, String partitionPath, String fileId,
                                    TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier);
  }

  public HoodieUnmergedCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable, String partitionPath, String fileId, TaskContextSupplier taskContextSupplier,
                                    boolean preserveMetadata) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier, preserveMetadata);
  }

  public HoodieUnmergedCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable, String partitionPath, String fileId,
                                    Option<Schema> overriddenSchema, TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, overriddenSchema, taskContextSupplier);
  }

  public HoodieUnmergedCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                    String partitionPath, String fileId, Iterator<BaseHoodieRecord> recordItr,
                                    TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier, true);
    unmergedRecordsIter = recordItr;
    useWriterSchema = true;
  }

  @Override
  public void write() {
    // Iterate all unmerged records and write them out
    if (!unmergedRecordsIter.hasNext()) {
      LOG.info("No unmerged records to write for partition path " + partitionPath + " and fileId " + fileId);
      return;
    }
    while (unmergedRecordsIter.hasNext()) {
      BaseHoodieRecord unmergedRecord = unmergedRecordsIter.next();
      merge(unmergedRecord);
    }

    // Write out the last merged record
    if (currentMergedRecord.isPresent()) {
      write(currentMergedRecord.get(), writeSchemaWithMetaFields, config.getProps());
    }
  }

  private void merge(BaseHoodieRecord newRecord) {

    // for delete record, skip merge logic
    if (newRecord instanceof DeleteRecord) {
      if (currentMergedRecord.isEmpty() || !currentMergedRecord.get().getKey().getRecordKey().equals(newRecord.getKey().getRecordKey())) {
        throw new HoodieUpsertException("Delete not supported in MergeOnRead");
      }
      recordsDeleted++;
      currentMergedRecord = Option.empty();
      return;
    }

    // for update record, merge new record with current merged record
    HoodieRecord<T> record = (HoodieRecord<T>) newRecord;
    if (currentMergedRecord.isEmpty()) {
      currentMergedRecord = Option.of(record);
      currentMergedRecordMerged = false;
      return;
    }
    if (record.getRecordKey().equals(currentMergedRecord.get().getRecordKey())) {
      // run merge logic when two records have the same key
      Schema oldSchema = writeSchemaWithMetaFields;
      Schema newSchema = preserveMetadata ? writeSchemaWithMetaFields : writeSchema;
      TypedProperties props = config.getPayloadConfig().getProps();
      Option<Pair<HoodieRecord, Schema>> mergeResult;
      try {
        mergeResult = recordMerger.merge(currentMergedRecord.get(), oldSchema, record, newSchema, props);
      } catch (Exception e) {
        throw new HoodieUpsertException("Merge failed for key " + record.getRecordKey(), e);
      }
      // TODO: deal with complex merge result
      Option<HoodieRecord> combinedRecord = mergeResult.map(Pair::getLeft);
      if (!combinedRecord.isPresent()) {
        // directly throw exception when merge result is empty
        throw new HoodieUpsertException("Merge failed for key " + record.getRecordKey());
      }
      combinedRecord.get().setKey(currentMergedRecord.get().getKey());
      currentMergedRecord = Option.of(combinedRecord.get());
      currentMergedRecordMerged = true;
      return;
    }

    // different keys, write current merged record and update it with new record
    write(currentMergedRecord.get(), writeSchemaWithMetaFields, config.getProps());
    currentMergedRecord = Option.of(record);
    currentMergedRecordMerged = false;
  }
}
