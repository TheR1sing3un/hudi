/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.benchmark

import org.apache.hadoop.fs.Path
import org.apache.hudi.DefaultSparkRecordMerger
import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.model.HoodieAvroRecordMerger
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.internal.schema.Types.IntType
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.spark.SparkConf
import org.apache.spark.hudi.benchmark.{HoodieBenchmark, HoodieBenchmarkBase}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, Row, RowFactory, SparkSession}

import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

object PartialUpdateNativeAndAvroBenchmark extends HoodieBenchmarkBase {

  protected val spark: SparkSession = getSparkSession
  private val avroTable = "avro_merger_table"
  private val sparkTable = "spark_merger_table"

  def getSparkSession: SparkSession = SparkSession
    .builder()
    .master("local[4]")
    .appName(this.getClass.getCanonicalName)
    .withExtensions(new HoodieSparkSessionExtension)
    .config("spark.driver.memory", "4G")
    .config("spark.executor.memory", "4G")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .config("hoodie.insert.shuffle.parallelism", "2")
    .config("hoodie.upsert.shuffle.parallelism", "2")
    .config("hoodie.delete.shuffle.parallelism", "2")
    .config("spark.sql.session.timeZone", "CTT")
    .config("spark.kryoserializer.buffer.max", "2000m")
    .config("spark.sql.filesourceTableRelationCacheSize", "0")
    .config(sparkConf())
    .getOrCreate()

  def sparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
    sparkConf
  }

  private def getData(startId: Int, endId: Int, numColumns: Int): DataFrame = {
    val schema = StructType(Seq(StructField("id", IntegerType, true), StructField("order", IntegerType, true)) ++ (1 to numColumns).map(i => s"c$i").map(name => StructField(name, StringType, true)))

    val javaList = new java.util.ArrayList[Row](endId - startId)
    for (i <- startId until endId) {
      val values = new java.util.ArrayList[Any](2 + numColumns)
      values.add(i)
      values.add(1)
      for (j <- 0 until numColumns) {
        values.add(s"v$j${UUID.randomUUID().toString}")
      }
      javaList.add(RowFactory.create(values.toArray: _*))
    }

    spark.createDataFrame(javaList, schema)
  }

  private def createWideDataFrame(rowNum: Long): DataFrame = {
    val colNames = (1 to 1000).map(i => s"c$i").toSeq
    val colValues = (1 to 1000).map(i => lit(s"v$i")).toSeq
    val df = spark.range(0, rowNum).toDF("id")
      .withColumn("order", lit(1))
      .withColumns(colNames, colValues)
    df
  }

  private def createSubSetOfWideDataFrame(rowNum: Long, columnCount: Int): DataFrame = {
    val colNames = (1 to columnCount).map(i => s"c$i").toSeq
    val colValues = (1 to columnCount).map(i => lit(s"v$i")).toSeq
    val df = spark.range(0, rowNum).toDF("id")
      .withColumn("order", lit(1))
      .withColumns(colNames, colValues)
    df
  }

  def createTable(tableName: String, writeOptions: String, tableBasePath: String): Unit = {

    val columns = (1 to 1000).map(i => s"c$i string").mkString(",\n  ")

    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  order int,
         |  $columns
         |) using hudi
         | $writeOptions
         | location '$tableBasePath'
   """.stripMargin)

  }

  def getWriteOptions(tableName: String, mergeMode: String, optimized: Boolean = false): String = {
    val (payload, logFormat) = if (mergeMode.contains("Spark")) {
      // spark record mode
      ("", s"${HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key} = 'parquet',")
    } else {
      (s"${HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key()} = 'org.apache.hudi.common.model.PartialUpdateAvroPayload',", "")
    }
    val mergeImpl = s"${HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key()} = '$mergeMode',"
    s"""
       |tblproperties (
       |  type = 'mor',
       |  primaryKey = 'id',
       |  preCombineField = 'order',
       |  hoodie.database.name = "default",
       |  hoodie.index.type = "BUCKET",
       |  hoodie.bucket.index.num.buckets = 1,
       |  hoodie.write.concurrency.mode = 'NON_BLOCKING_CONCURRENCY_CONTROL',
       |  hoodie.write.lock.provider = 'org.apache.hudi.client.transaction.lock.InProcessLockProvider',
       |  hoodie.write.schema.engine.specific.optimized.enabled = '$optimized',
       |  $payload
       |  $logFormat
       |  $mergeImpl
       |  hoodie.table.name = "$tableName"
       | )""".stripMargin
  }

  private def dropTable(tableName: String): Unit = {
    if (spark.catalog.tableExists(tableName)) {
      spark.sql(s"drop table if exists $tableName").count()
    }
  }

  private def createHoodieTable(tableName: String, path: String, mergeMode: String, optimized: Boolean = false): Unit = {
    val writeOptions = getWriteOptions(tableName, mergeMode, optimized)
    createTable(tableName, writeOptions, path)
  }

  private def bulkInsertHoodieTable(tableName: String, df: DataFrame): Unit = {
    df.collect()
    df.createOrReplaceTempView("input_df")
    spark.sql(s"set hoodie.datasource.write.operation = bulk_insert")
    spark.sql(s"insert into table $tableName select * from input_df")
    spark.conf.unset("hoodie.datasource.write.operation")
  }

  private def upsertHoodieTable(tableName: String, df: DataFrame): Unit = {
    df.collect()
    df.createOrReplaceTempView("input_df")

    spark.sql(s"insert into table $tableName select * from input_df")
  }

  /**
   * OpenJDK 64-Bit Server VM 1.8.0_422-b05 on Mac OS X 14.4
   * Apple M3 Pro
   * perf bulk insert:                                         Best Time(ms)   Avg Time(ms)   Stdev(ms)    Rate(M/s)   Per Row(ns)   Relative
   * -----------------------------------------------------------------------------------------------------------------------------------
   * org.apache.hudi.common.model.HoodieAvroRecordMerger          14461          22012         NaN          0.0      482024.8       1.0X
   * org.apache.hudi.DefaultSparkRecordMerger                     14732          16375        2044          0.0      491058.0       1.0X
   */
  private def bulkInsertBenchmark(): Unit = {
    val df = createWideDataFrame(30000)
    val avroMergerImpl = classOf[HoodieAvroRecordMerger].getName
    val sparkMergerImpl = classOf[DefaultSparkRecordMerger].getName
    val benchmark = new HoodieBenchmark("perf bulk insert", 30000, 3)
    Seq(avroMergerImpl, sparkMergerImpl).foreach {
      case mergeMode => benchmark.addCase(mergeMode) { _ =>
        withTempDir { f =>
          val tableName = s"table_${Random.nextInt(1000)}"
          createHoodieTable(tableName, new Path(f.getCanonicalPath, tableName).toUri.toString, mergeMode)
          bulkInsertHoodieTable(tableName, df)
        }
      }
    }
    benchmark.run()
  }

  /**
   * OpenJDK 64-Bit Server VM 1.8.0_422-b05 on Mac OS X 14.4
   * Apple M3 Pro
   * perf upsert:                                         Best Time(ms)   Avg Time(ms)   Stdev(ms)    Rate(M/s)   Per Row(ns)   Relative
   * -----------------------------------------------------------------------------------------------------------------------------------
   * org.apache.hudi.common.model.HoodieAvroRecordMerger          17971          20446        1669          0.0      599021.1       1.0X
   * org.apache.hudi.DefaultSparkRecordMerger                     30517          34156        1294          0.0     1017237.2       0.6X
   */
  private def upsertBenchmark(): Unit = {
    val df = createWideDataFrame(30000)
    val avroMergerImpl = classOf[HoodieAvroRecordMerger].getName
    val sparkMergerImpl = classOf[DefaultSparkRecordMerger].getName
    val benchmark = new HoodieBenchmark("perf upsert", 30000, 3)
    Seq(avroMergerImpl, sparkMergerImpl).foreach {
      case mergeMode => benchmark.addCase(mergeMode) { _ =>
        withTempDir { f =>
          val tableName = s"table_${Random.nextInt(1000)}"
          createHoodieTable(tableName, new Path(f.getCanonicalPath, tableName).toUri.toString, mergeMode)
          upsertHoodieTable(tableName, df)
        }
      }
    }
    benchmark.run()
  }

  /**
   * OpenJDK 64-Bit Server VM 1.8.0_422-b05 on Mac OS X 14.4
   * Apple M3 Pro
   * perf partial upsert:                                 Best Time(ms)   Avg Time(ms)   Stdev(ms)    Rate(M/s)   Per Row(ns)   Relative
   * -----------------------------------------------------------------------------------------------------------------------------------
   * org.apache.hudi.common.model.HoodieAvroRecordMerger           4300           5213         839          0.0      143323.2       1.0X
   * org.apache.hudi.DefaultSparkRecordMerger                      6310           6769         447          0.0      210325.8       0.7X
   */
  private def partialUpsertBenchmark100(): Unit = {
    val df = createSubSetOfWideDataFrame(30000, 100)
    val avroMergerImpl = classOf[HoodieAvroRecordMerger].getName
    val sparkMergerImpl = classOf[DefaultSparkRecordMerger].getName
    val benchmark = new HoodieBenchmark("perf partial upsert", 30000, 3)
    Seq(avroMergerImpl, sparkMergerImpl).foreach {
      case mergeMode => benchmark.addCase(mergeMode) { _ =>
        withTempDir { f =>
          val tableName = s"table_${Random.nextInt(1000)}"
          createHoodieTable(tableName, new Path(f.getCanonicalPath, tableName).toUri.toString, mergeMode)
          upsertHoodieTable(tableName, df)
        }
      }
    }
    benchmark.run()
  }

  /**
   * OpenJDK 64-Bit Server VM 1.8.0_422-b05 on Mac OS X 14.4
   * Apple M3 Pro
   * perf partial upsert:                                        Best Time(ms)   Avg Time(ms)   Stdev(ms)    Rate(M/s)   Per Row(ns)   Relative
   * ------------------------------------------------------------------------------------------------------------------------------------------
   * org.apache.hudi.common.model.HoodieAvroRecordMerger : 1              3153           3873         972          0.0      157639.7       1.0X
   * org.apache.hudi.common.model.HoodieAvroRecordMerger : 10             3267           3402         122          0.0      163339.9       1.0X
   * org.apache.hudi.common.model.HoodieAvroRecordMerger : 100            7089           7767         653          0.0      354431.0       0.4X
   * org.apache.hudi.common.model.HoodieAvroRecordMerger : 300           32461          33332        1022          0.0     1623033.4       0.1X
   * org.apache.hudi.common.model.HoodieAvroRecordMerger : 1000         140676         179876         NaN          0.0     7033820.1       0.0X
   * org.apache.hudi.DefaultSparkRecordMerger : 1                         3810           4317         831          0.0      190499.3       0.8X
   * org.apache.hudi.DefaultSparkRecordMerger : 10                        3380           3742         317          0.0      169013.5       0.9X
   * org.apache.hudi.DefaultSparkRecordMerger : 100                      12117          12640         631          0.0      605834.7       0.3X
   * org.apache.hudi.DefaultSparkRecordMerger : 300                      45792          60876         NaN          0.0     2289616.1       0.1X
   * org.apache.hudi.DefaultSparkRecordMerger : 1000                    170018         187090        1082          0.0     8500898.0       0.0X
   */
  private def partialUpsertBenchmark(): Unit = {
    val num = 20000;
    val df1 = getData(0, num, 1)
    val df10 = getData(0, num, 10)
    val df100 = getData(0, num, 100)
    val df300 = getData(0, num, 300)
    val df1000 = getData(0, num, 1000)
    val avroMergerImpl = classOf[HoodieAvroRecordMerger].getName
    val sparkMergerImpl = classOf[DefaultSparkRecordMerger].getName
    val benchmark = new HoodieBenchmark("perf partial upsert", num, 3)
    val cases = Seq(
      (avroMergerImpl, df1, 1),
      (avroMergerImpl, df10, 10),
      (avroMergerImpl, df100, 100),
      (avroMergerImpl, df300, 300),
      (avroMergerImpl, df1000, 1000),
      (sparkMergerImpl, df1, 1),
      (sparkMergerImpl, df10, 10),
      (sparkMergerImpl, df100, 100),
      (sparkMergerImpl, df300, 300),
      (sparkMergerImpl, df1000, 1000)
    )
    cases.foreach {
      case (mergeMode, df, columnNum) => benchmark.addCase(s"$mergeMode : $columnNum") { _ =>
        withTempDir { f =>
          val tableName = s"table_${columnNum}_${Random.nextInt(1000)}"
          createHoodieTable(tableName, new Path(f.getCanonicalPath, tableName).toUri.toString, mergeMode)
          upsertHoodieTable(tableName, df)
        }
      }
    }
    benchmark.run()
  }


  /**
   * OpenJDK 64-Bit Server VM 1.8.0_422-b05 on Mac OS X 14.4
   * Apple M3 Pro
   * perf partial upsert:                             Best Time(ms)   Avg Time(ms)   Stdev(ms)    Rate(M/s)   Per Row(ns)   Relative
   * -------------------------------------------------------------------------------------------------------------------------------
   * org.apache.hudi.DefaultSparkRecordMerger : 1000         133045         164048         914          0.0     6652244.2       1.0X
   *
   * org.apache.hudi.DefaultSparkRecordMerger : 1000         163541         174863        1986          0.0     8177036.5       1.0X
   */
  private def partialUpsertBenchmarkSparkRecordPerf(): Unit = {
    val num = 100000;
//    val df1 = getData(0, num, 1)
//    val df10 = getData(0, num, 10)
    val df100 = getData(0, num, 100)
//    val df300 = getData(0, num, 300)
    // val df1000 = getData(0, num, 1000)
    val avroMergerImpl = classOf[HoodieAvroRecordMerger].getName
    val sparkMergerImpl = classOf[DefaultSparkRecordMerger].getName
    val benchmark = new HoodieBenchmark("perf partial upsert", num, 5)
    val cases = Seq(
//      (avroMergerImpl, df1, 1),
//      (avroMergerImpl, df10, 10),
//      (avroMergerImpl, df100, 100),
//      (avroMergerImpl, df300, 300),
      //(avroMergerImpl, df1000, 1000),
//      (sparkMergerImpl, df1, 1),
//      (sparkMergerImpl, df10, 10),
//      (sparkMergerImpl, df100, 100),
//      (sparkMergerImpl, df300, 300),
      (sparkMergerImpl, df100, 100, false),
      (sparkMergerImpl, df100, 100, true)
    )
    cases.foreach {
      case (mergeMode, df, columnNum, optimized) => benchmark.addCase(s"$mergeMode : $columnNum, optimized: $optimized") { _ =>
        withTempDir { f =>
          val tableName = s"table_${columnNum}_${Random.nextInt(1000)}"
          createHoodieTable(tableName, new Path(f.getCanonicalPath, tableName).toUri.toString, mergeMode, optimized)
          upsertHoodieTable(tableName, df)
        }
      }
    }
    benchmark.run()
  }

  /**
   * OpenJDK 64-Bit Server VM 1.8.0_422-b05 on Mac OS X 14.4
   * Apple M3 Pro
   * perf optimized read:
   * Table Type          Query Type     Query Column Num    Best Time(ms)   Avg Time(ms)   Stdev(ms)    Rate(M/s)   Per Row(ns)   Relative
   * ---------------------------------------------------------------------------------------------------------------------------------------------
   * avro_merger_table : snapshot :           1                  69319          79563         341          0.0     2310632.7       1.0X
   * avro_merger_table : snapshot :           10                 67844          91346         474          0.0     2261464.4       1.0X
   * avro_merger_table : snapshot :           100                68120          80277         NaN          0.0     2270681.2       1.0X
   * avro_merger_table : snapshot :           1000               67793          80644         NaN          0.0     2259777.2       1.0X
   * avro_merger_table : read_optimized :     1                    224            335          66          0.1        7450.7     310.1X
   * avro_merger_table : read_optimized :     10                   182            326         114          0.2        6065.1     381.0X
   * avro_merger_table : read_optimized :     100                  191            261          62          0.2        6367.1     362.9X
   * avro_merger_table : read_optimized :     1000                 282            395         110          0.1        9402.6     245.7X
   * spark_merger_table : snapshot :          1                   8985          10885         NaN          0.0      299506.4       7.7X
   * spark_merger_table : snapshot :          10                  8705          11479         856          0.0      290174.9       8.0X
   * spark_merger_table : snapshot :          100                 9553          10019         324          0.0      318428.4       7.3X
   * spark_merger_table : snapshot :          1000                8211          10528         909          0.0      273700.0       8.4X
   * spark_merger_table : read_optimized :    1                    199            290          82          0.2        6623.8     348.8X
   * spark_merger_table : read_optimized :    10                   189            329         125          0.2        6300.3     366.8X
   * spark_merger_table : read_optimized :    100                  224            323         116          0.1        7458.8     309.8X
   * spark_merger_table : read_optimized :    1000                 252            375         171          0.1        8385.6     275.5X
   */
  private def readBenchmark(): Unit = {
    spark.conf.set("spark.sql.cache.enabled", "false")
    spark.conf.set("spark.sql.optimizer.cache.enabled", "false")
    val batchNum1 = 20000
    val batchNum2 = 10000
    var df = getData(0, batchNum1, 1000)
    val avroMergerImpl = classOf[HoodieAvroRecordMerger].getName
    val sparkMergerImpl = classOf[DefaultSparkRecordMerger].getName

    val avroTable = "avro_merger_table"
    val sparkTable = "spark_merger_table"

    withTempDir { f =>

      createHoodieTable(avroTable, new Path(f.getCanonicalPath, avroTable).toUri.toString, avroMergerImpl)
      createHoodieTable(sparkTable, new Path(f.getCanonicalPath, sparkTable).toUri.toString, sparkMergerImpl)

      spark.catalog.uncacheTable(avroTable)
      spark.catalog.uncacheTable(sparkTable)

      bulkInsertHoodieTable(avroTable, df)
      bulkInsertHoodieTable(sparkTable, df)

      // insert more data with full columns
      df = getData(batchNum1, batchNum1 + batchNum2, 1000)
      upsertHoodieTable(avroTable, df)
      upsertHoodieTable(sparkTable, df)

      // update data
      df = getData(0, batchNum1, 300)
      upsertHoodieTable(sparkTable, df)
      upsertHoodieTable(avroTable, df)

      // update more data
      df = getData(batchNum1, batchNum1 + batchNum2, 100)
      upsertHoodieTable(sparkTable, df)
      upsertHoodieTable(avroTable, df)


      val benchmark = new HoodieBenchmark("perf optimized read", 30000, 10)

      val snapshotRead = "snapshot"
      val readOptimized = "read_optimized"

      val columns1 = "c1"
      val columns10 = (1 to 10).map(i => s"c$i").mkString(",")
      val columns100 = (1 to 100).map(i => s"c$i").mkString(",")
      val columns1000 = (1 to 1000).map(i => s"c$i").mkString(",")

      val cases = Seq(
        (avroTable, snapshotRead, columns1),
        (avroTable, snapshotRead, columns10),
        (avroTable, snapshotRead, columns100),
        (avroTable, snapshotRead, columns1000),
        (avroTable, readOptimized, columns1),
        (avroTable, readOptimized, columns10),
        (avroTable, readOptimized, columns100),
        (avroTable, readOptimized, columns1000),
        (sparkTable, snapshotRead, columns1),
        (sparkTable, snapshotRead, columns10),
        (sparkTable, snapshotRead, columns100),
        (sparkTable, snapshotRead, columns1000),
        (sparkTable, readOptimized, columns1),
        (sparkTable, readOptimized, columns10),
        (sparkTable, readOptimized, columns100),
        (sparkTable, readOptimized, columns1000)
      )

      cases.foreach {
        case (table, queryType, columns) => {
          val columnNum = columns.split(",").length
          benchmark.addCase(s"$table : $queryType : $columnNum") { _ =>
            spark.sql(s"set hoodie.datasource.query.type = $queryType")
            val count = spark.sql(s"select id, order, $columns from $table").count()
            spark.conf.unset("hoodie.datasource.query.type")
          }
        }
      }
      benchmark.run()
    }
  }

  private def allFlowBenchmark(): Unit = {
    val avroMergerImpl = classOf[HoodieAvroRecordMerger].getName
    val sparkMergerImpl = classOf[DefaultSparkRecordMerger].getName
    val df = createWideDataFrame(10000)
    withTempDir { avroPath =>
      withTempDir { sparkPath =>
        val sparkBasePath = new Path(sparkPath.getCanonicalPath, sparkTable).toUri.toString
        val avroBasePath = new Path(avroPath.getCanonicalPath, avroTable).toUri.toString

        // 1. bulk insert
        val bulkInsertBenchmark = new HoodieBenchmark("perf bulk insert", 10000, 1, warmupTime = FiniteDuration.apply(0, "ms"))
        Seq((avroMergerImpl, avroTable, avroBasePath), (sparkMergerImpl, sparkTable, sparkBasePath))
          .foreach {
            case (mergerImpl, tableName, tablePath) => bulkInsertBenchmark.addCase(mergerImpl) { _ =>
              dropTable(tableName)
              createHoodieTable(tableName, tablePath, mergerImpl)
              bulkInsertHoodieTable(tableName, df)
            }
          }
        bulkInsertBenchmark.run()

        // 2. partial update
        val partialUpdateBenchmark = new HoodieBenchmark("perf partial update", 10000, 1, warmupTime = FiniteDuration.apply(0, "ms"))
        Seq((avroMergerImpl, avroTable, avroBasePath), (sparkMergerImpl, sparkTable, sparkBasePath))
          .foreach {
            case (mergerImpl, tableName, tablePath) => partialUpdateBenchmark.addCase(mergerImpl) { _ =>
              upsertHoodieTable(tableName, df)
            }
          }
        partialUpdateBenchmark.run()

        // 3. snapshot read
        val snapshotReadBenchmark = new HoodieBenchmark("perf snapshot read", 10000, 1, warmupTime = FiniteDuration.apply(0, "ms"))
        Seq((avroMergerImpl, avroTable), (sparkMergerImpl, sparkTable))
          .foreach {
            case (mergerImpl, tableName) => snapshotReadBenchmark.addCase(mergerImpl) { _ =>
              spark.sql(s"select * from $tableName").collect()
            }
          }
        snapshotReadBenchmark.run()

        // 4. compaction
        val compactionBenchmark = new HoodieBenchmark("perf compaction", 10000, 1, warmupTime = FiniteDuration.apply(0, "ms"))
        Seq((avroMergerImpl, avroTable, avroBasePath), (sparkMergerImpl, sparkTable, sparkBasePath))
          .foreach {
            case (mergerImpl, tableName, tablePath) => compactionBenchmark.addCase(mergerImpl) { _ =>
              spark.sql(s"set hoodie.compaction.strategy = org.apache.hudi.table.action.compact.strategy.UnBoundedCompactionStrategy")
              spark.sql(s"set hoodie.compact.inline.trigger.strategy = NUM_COMMITS")
              spark.sql(s"set hoodie.compact.inline.max.delta.commits = 0")
              spark.sql(s"run compaction on $tableName")
            }
          }
        compactionBenchmark.run()

        // 5. read after compaction
        val readAfterCompactionBenchmark = new HoodieBenchmark("perf read after compaction", 10000, 1, warmupTime = FiniteDuration.apply(0, "ms"))
        Seq((avroMergerImpl, avroTable), (sparkMergerImpl, sparkTable))
          .foreach {
            case (mergerImpl, tableName) => readAfterCompactionBenchmark.addCase(mergerImpl) { _ =>
              spark.sql(s"select * from $tableName").collect()
            }
          }
        readAfterCompactionBenchmark.run()

        // output
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    //    overwriteBenchmark()
    //    upsertThenReadBenchmark()
    // upsertBenchmark()
    //partialUpsertBenchmark()
    partialUpsertBenchmarkSparkRecordPerf
    // partialUpsertBenchmark()
    //allFlowBenchmark()
    // readBenchmark()
  }
}
