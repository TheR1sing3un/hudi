/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.HoodieSparkConfUtils.getHollowCommitHandling
import org.apache.hudi.common.model.{FileSlice, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling.USE_TRANSITION_TIME
import org.apache.hudi.common.table.timeline.TimelineUtils.{HollowCommitHandling, getCommitMetadata, handleHollowCommitIfNeeded}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.listAffectedFilesForCommits
import org.apache.hudi.metadata.HoodieTableMetadataUtil.getWritePartitionPaths
import org.apache.hudi.storage.StoragePathInfo

import org.apache.hadoop.fs.GlobPattern
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.immutable

/**
 * @Experimental
 */
case class MergeOnReadIncrementalRelation(override val sqlContext: SQLContext,
                                          override val optParams: Map[String, String],
                                          override val metaClient: HoodieTableMetaClient,
                                          private val userSchema: Option[StructType],
                                          private val prunedDataSchema: Option[StructType] = None)
  extends BaseMergeOnReadSnapshotRelation(sqlContext, optParams, metaClient, Seq(), userSchema, prunedDataSchema)
    with HoodieIncrementalRelationTrait {

  override type Relation = MergeOnReadIncrementalRelation

  override def updatePrunedDataSchema(prunedSchema: StructType): Relation =
    this.copy(prunedDataSchema = Some(prunedSchema))

  override protected def timeline: HoodieTimeline = {
    if (fullTableScan) {
      handleHollowCommitIfNeeded(metaClient.getCommitsAndCompactionTimeline, metaClient, hollowCommitHandling)
    } else if (hollowCommitHandling == HollowCommitHandling.USE_TRANSITION_TIME) {
      metaClient.getCommitsAndCompactionTimeline.findInstantsInRangeByStateTransitionTime(startTimestamp, endTimestamp)
    } else {
      handleHollowCommitIfNeeded(metaClient.getCommitsAndCompactionTimeline, metaClient, hollowCommitHandling)
        .findInstantsInRange(startTimestamp, endTimestamp)
    }
  }

  protected override def composeRDD(partitions: Seq[HoodieDefaultFilePartition],
                                    tableSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    requestedColumns: Array[String],
                                    filters: Array[Filter]): RDD[InternalRow] = {
    // The only required filters are ones that make sure we're only fetching records that
    // fall into incremental span of the timeline being queried
    val requiredFilters = incrementalSpanRecordFilters
    val optionalFilters = filters
    val readers = createBaseFileReaders(tableSchema, requiredSchema, requestedColumns, requiredFilters, optionalFilters)

    new HoodieMergeOnReadRDD(
      sqlContext.sparkContext,
      config = jobConf,
      fileReaders = readers,
      tableSchema = tableSchema,
      requiredSchema = requiredSchema,
      tableState = tableState,
      mergeType = mergeType,
      partitions = partitions,
      includeStartTime = includeStartTime,
      startTimestamp = startTs,
      endTimestamp = endTs)
  }

  override protected def collectFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): List[HoodieMergeOnReadFileSplit] = {
    if (includedCommits.isEmpty) {
      List()
    } else {
      val fileSlices = if (fullTableScan) {
        listLatestFileSlices(Seq(), partitionFilters, dataFilters)
      } else {
        val latestCommit = includedCommits.last.getTimestamp

        val fsView = new HoodieTableFileSystemView(
          metaClient, timeline, affectedFilesInCommits)

        val modifiedPartitions = getWritePartitionPaths(commitsMetadata)

        modifiedPartitions.asScala.flatMap { relativePartitionPath =>
          fsView.getLatestMergedFileSlicesBeforeOrOn(relativePartitionPath, latestCommit).iterator().asScala
        }.toSeq
      }

      buildSplits(filterFileSlices(fileSlices, globPattern))
    }
  }

  private def filterFileSlices(fileSlices: Seq[FileSlice], pathGlobPattern: String): Seq[FileSlice] = {
    val filteredFileSlices = if (!StringUtils.isNullOrEmpty(pathGlobPattern)) {
      val globMatcher = new GlobPattern("*" + pathGlobPattern)
      fileSlices.filter(fileSlice => {
        val path = toScalaOption(fileSlice.getBaseFile).map(_.getPath)
          .orElse(toScalaOption(fileSlice.getLatestLogFile).map(_.getPath.toString))
          .get
        globMatcher.matches(path)
      })
    } else {
      fileSlices
    }
    filteredFileSlices
  }
}

trait HoodieIncrementalRelationTrait extends HoodieBaseRelation {

  // Validate this Incremental implementation is properly configured
  validate()

  protected val hollowCommitHandling: HollowCommitHandling = getHollowCommitHandling(optParams)

  protected def startTimestamp: String = optParams(DataSourceReadOptions.BEGIN_INSTANTTIME.key)

  protected def endTimestamp: String = optParams.getOrElse(
    DataSourceReadOptions.END_INSTANTTIME.key,
    if (hollowCommitHandling == USE_TRANSITION_TIME) super.timeline.lastInstant().get.getStateTransitionTime
    else super.timeline.lastInstant().get.getTimestamp)

  protected def startInstantArchived: Boolean = super.timeline.isBeforeTimelineStarts(startTimestamp)

  protected def endInstantArchived: Boolean = super.timeline.isBeforeTimelineStarts(endTimestamp)

  // Fallback to full table scan if any of the following conditions matches:
  //   1. the start commit is archived
  //   2. the end commit is archived
  //   3. there are files in metadata be deleted
  protected lazy val fullTableScan: Boolean = {
    val fallbackToFullTableScan = optParams.getOrElse(DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.key,
      DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.defaultValue).toBoolean

    fallbackToFullTableScan && (startInstantArchived || endInstantArchived
      || affectedFilesInCommits.asScala.exists(fileStatus => !metaClient.getStorage.exists(fileStatus.getPath)))
  }

  protected lazy val includedCommits: immutable.Seq[HoodieInstant] = {
    if (!startInstantArchived || !endInstantArchived) {
      // If endTimestamp commit is not archived, will filter instants
      // before endTimestamp.
      if (hollowCommitHandling == USE_TRANSITION_TIME) {
        super.timeline.findInstantsInRangeByStateTransitionTime(startTimestamp, endTimestamp).getInstants.asScala.toList
      } else {
        super.timeline.findInstantsInRange(startTimestamp, endTimestamp).getInstants.asScala.toList
      }
    } else {
      super.timeline.getInstants.asScala.toList
    }
  }

  protected lazy val commitsMetadata = includedCommits.map(getCommitMetadata(_, super.timeline)).asJava

  protected lazy val affectedFilesInCommits: java.util.List[StoragePathInfo] = {
    listAffectedFilesForCommits(conf, metaClient.getBasePath, commitsMetadata)
  }

  protected lazy val (includeStartTime, startTs) = if (startInstantArchived) {
    (false, startTimestamp)
  } else {
    (true, includedCommits.head.getTimestamp)
  }
  protected lazy val endTs: String = if (endInstantArchived) endTimestamp else includedCommits.last.getTimestamp

  // Record filters making sure that only records w/in the requested bounds are being fetched as part of the
  // scan collected by this relation
  protected lazy val incrementalSpanRecordFilters: Seq[Filter] = {
    val isNotNullFilter = IsNotNull(HoodieRecord.COMMIT_TIME_METADATA_FIELD)

    val largerThanFilter = if (includeStartTime) {
      GreaterThanOrEqual(HoodieRecord.COMMIT_TIME_METADATA_FIELD, startTs)
    } else {
      GreaterThan(HoodieRecord.COMMIT_TIME_METADATA_FIELD, startTs)
    }

    val lessThanFilter = LessThanOrEqual(HoodieRecord.COMMIT_TIME_METADATA_FIELD, endTs)

    Seq(isNotNullFilter, largerThanFilter, lessThanFilter)
  }

  override lazy val mandatoryFields: Seq[String] = {
    // NOTE: This columns are required for Incremental flow to be able to handle the rows properly, even in
    //       cases when no columns are requested to be fetched (for ex, when using {@code count()} API)
    Seq(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD) ++
      preCombineFieldOpt.map(Seq(_)).getOrElse(Seq())
  }

  protected def validate(): Unit = {
    if (super.timeline.empty()) {
      throw new HoodieException("No instants to incrementally pull")
    }

    if (!this.optParams.contains(DataSourceReadOptions.BEGIN_INSTANTTIME.key)) {
      throw new HoodieException(s"Specify the begin instant time to pull from using " +
        s"option ${DataSourceReadOptions.BEGIN_INSTANTTIME.key}")
    }

    if (!this.tableConfig.populateMetaFields()) {
      throw new HoodieException("Incremental queries are not supported when meta fields are disabled")
    }

    if (hollowCommitHandling == USE_TRANSITION_TIME && fullTableScan) {
      throw new HoodieException("Cannot use stateTransitionTime while enables full table scan")
    }
  }

  protected def globPattern: String =
    optParams.getOrElse(DataSourceReadOptions.INCR_PATH_GLOB.key, DataSourceReadOptions.INCR_PATH_GLOB.defaultValue)

}

