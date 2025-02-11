/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.HoodieDataSourceHelpers.{hasNewCommits, latestCompletedCommit, listCommitsSince, streamCompletedInstantSince}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.WriteOperationType.{BULK_INSERT, INSERT, UPSERT}
import org.apache.hudi.common.model.{HoodieRecord, WriteOperationType}
import org.apache.hudi.common.table.timeline.TimelineUtils
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.scalatest.Inspectors.forAll

import java.io.File
import scala.collection.JavaConverters._

@SparkSQLCoreFlow
class TestSparkPartialUpdateFlow extends HoodieSparkSqlTestBase {
  val colsToCompare = "timestamp, _row_key, partition_path, rider, driver, begin_lat, begin_lon, end_lat, end_lon, fare.amount, fare.currency, _hoodie_is_deleted"

  //params for core flow tests
  val params: List[String] = List(
    "true"
    //"false"
//    "COPY_ON_WRITE|false|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_BLOOM",
//    "COPY_ON_WRITE|true|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_BLOOM",
//    "COPY_ON_WRITE|false|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_SIMPLE",
//    "COPY_ON_WRITE|true|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_SIMPLE",
//    "COPY_ON_WRITE|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|BLOOM",
//    "COPY_ON_WRITE|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|BLOOM",
//    "COPY_ON_WRITE|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|SIMPLE",
//    "COPY_ON_WRITE|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|SIMPLE",
//    "MERGE_ON_READ|false|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_BLOOM",
//    "MERGE_ON_READ|true|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_BLOOM",
//    "MERGE_ON_READ|false|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_SIMPLE",
//    "MERGE_ON_READ|true|org.apache.hudi.keygen.SimpleKeyGenerator|GLOBAL_SIMPLE",
//    "MERGE_ON_READ|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|BLOOM",
//    "MERGE_ON_READ|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|BLOOM",
//    "MERGE_ON_READ|false|org.apache.hudi.keygen.NonpartitionedKeyGenerator|SIMPLE",
//    "MERGE_ON_READ|true|org.apache.hudi.keygen.NonpartitionedKeyGenerator|SIMPLE"
  )

  //extracts the params and runs each core flow test
  forAll (params) { (paramStr: String) =>
    test(s"Core flow with params: $paramStr") {
      val splits = paramStr.split('|')
      withTempDir { basePath =>
        testCoreFlows(basePath,
          useSparkRecord = splits(0).toBoolean)
      }
    }
  }

  def testCoreFlows(basePath: File, useSparkRecord: Boolean): Unit = {
    //Create table and set up for testing
    val tableName = generateTableName
    val tableBasePath = basePath.getCanonicalPath + "/" + tableName
    val writeOptions = getWriteOptions(tableName)
    createTable(tableName, writeOptions, tableBasePath)

    spark.sql(s"set hoodie.datasource.query.type=$QUERY_TYPE_SNAPSHOT_OPT_VAL")

    // Bulk insert first set of records
    spark.sql("set hoodie.sql.bulk.insert.enable=true")
    spark.sql(s"insert into $tableName(_row_key, rider, driver, begin_lat, begin_lon, ts) values ('000', 'rider-000', 'driver-000', 0, 0, 0)").collect()
    spark.conf.unset("hoodie.sql.bulk.insert.enable")

    // Upsert, produce a log file

    spark.sql(s"insert into $tableName(_row_key, rider, driver, begin_lat, begin_lon, ts) values ('000', 'rider-001', 'driver-001', 1, 1, 0)").collect()

    // Upsert, produce a log file

    spark.sql(s"insert into $tableName(_row_key, rider, driver, begin_lat, begin_lon, ts) values ('000', 'rider-002', 'driver-002', 2, 2, 0)").collect()

    // Query
    spark.sql(s"set hoodie.datasource.query.type=$QUERY_TYPE_SNAPSHOT_OPT_VAL")
    spark.sql(s"set hoodie.file.group.reader.enabled=false")
    val rows = spark.sql(s"select * from $tableName").collect()
    assertEquals(1, rows.length)
  }

  def getWriteOptions(tableName: String): String = {

    s"""
       |tblproperties (
       |  type = 'mor',
       |  primaryKey = '_row_key',
       |  preCombineField = 'ts',
       |  hoodie.bulkinsert.shuffle.parallelism = 4,
       |  hoodie.database.name = "databaseName",
       |  hoodie.delete.shuffle.parallelism = 2,
       |  hoodie.index.type = "BUCKET",
       |  hoodie.insert.shuffle.parallelism = 4,
       |  hoodie.table.name = "$tableName",
       |  hoodie.upsert.shuffle.parallelism = 4,
       |  hoodie.write.record.merge.mode = 'CUSTOM',
       |  hoodie.compaction.payload.class = 'org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload'
       | )""".stripMargin
  }

  def assertOperation(basePath: String, count: Int, operationType: WriteOperationType): Boolean = {
    val metaClient = createMetaClient(spark, basePath)
    val timeline = metaClient.getActiveTimeline.getAllCommitsTimeline
    assert(timeline.countInstants() == count)
    val latestCommit = timeline.lastInstant()
    assert(latestCommit.isPresent)
    assert(latestCommit.get().isCompleted)
    val metadata = TimelineUtils.getCommitMetadata(latestCommit.get(), timeline)
    metadata.getOperationType.equals(operationType)
  }

  def insertInto(tableName: String, tableBasePath: String, inputDf: sql.DataFrame, writeOp: WriteOperationType,
                 count: Int): Unit = {
    inputDf.select("timestamp", "_row_key", "rider", "driver", "begin_lat", "begin_lon", "end_lat", "end_lon", "fare",
      "_hoodie_is_deleted", "partition_path").createOrReplaceTempView("insert_temp_table")
    try {
      if (writeOp.equals(UPSERT)) {
        spark.sql("set hoodie.sql.bulk.insert.enable=false")
        spark.sql("set hoodie.sql.insert.mode=upsert")
        spark.sql(
          s"""
             | merge into $tableName as target
             | using insert_temp_table as source
             | on target._row_key = source._row_key and
             | target.partition_path = source.partition_path
             | when matched then update set *
             | when not matched then insert *
             | """.stripMargin)
      } else if (writeOp.equals(BULK_INSERT)) {
        spark.sql("set hoodie.sql.bulk.insert.enable=true")
        spark.sql("set hoodie.sql.insert.mode=non-strict")
        spark.sql(s"insert into $tableName select * from insert_temp_table")
      } else if (writeOp.equals(INSERT)) {
        spark.sql("set hoodie.sql.bulk.insert.enable=false")
        spark.sql("set hoodie.sql.insert.mode=non-strict")
        spark.sql(s"insert into $tableName select * from insert_temp_table")
      }
      assertOperation(tableBasePath, count, writeOp)
    } finally {
      spark.conf.unset("hoodie.metadata.enable")
      spark.conf.unset("hoodie.datasource.write.keygenerator.class")
      spark.conf.unset("hoodie.sql.bulk.insert.enable")
      spark.conf.unset("hoodie.sql.insert.mode")
    }
  }

  def createTable(tableName: String, writeOptions: String, tableBasePath: String): Unit = {

    spark.sql(
      s"""
         | create table $tableName (
         |  _row_key string,
         |  rider string,
         |  driver string,
         |  begin_lat double,
         |  begin_lon double,
         |  ts long
         |) using hudi
         | $writeOptions
         | location '$tableBasePath'
         |
    """.stripMargin)
  }

  def generateInserts(dataGen: HoodieTestDataGenerator, instantTime: String, n: Int): sql.DataFrame = {
    val recs = dataGen.generateInsertsNestedExample(instantTime, n)
    spark.read.json(spark.sparkContext.parallelize(recordsToStrings(recs).asScala.toSeq, 2))
  }

  def generateUniqueUpdates(dataGen: HoodieTestDataGenerator, instantTime: String, n: Int): sql.DataFrame = {
    val recs = dataGen.generateUniqueUpdatesNestedExample(instantTime, n)
    spark.read.json(spark.sparkContext.parallelize(recordsToStrings(recs).asScala.toSeq, 2))
  }

  // Helper function to check if two rows are equal (comparing only the columns we care about)
  def rowsEqual(row1: Row, row2: Row): Boolean = {
    // Get schemas from rows
    val schema1 = row1.asInstanceOf[GenericRowWithSchema].schema
    val schema2 = row2.asInstanceOf[GenericRowWithSchema].schema

    // Verify schemas are identical
    if (schema1 != schema2) {
      throw new AssertionError(
        s"""Schemas are different:
            |Schema 1: ${schema1.treeString}
            |Schema 2: ${schema2.treeString}""".stripMargin)
    }

    // Compare all fields using schema
    schema1.fields.forall { field =>
      val idx1 = row1.fieldIndex(field.name)
      val idx2 = row2.fieldIndex(field.name)
      row1.get(idx1) == row2.get(idx2)
    }
  }
}
