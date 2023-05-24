package org.seabow.spark.v2.lucene

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.AggregatePushDownUtils
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.execution.datasources.{PartitioningAwareFileIndex, PartitioningUtils}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v3.evolving.expressions.aggregate.Aggregation
import org.apache.spark.sql.v3.evolving.{SupportsLucenePushDownFilters, SupportsPushDownAggregates}

import scala.collection.JavaConverters._

case class LuceneScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap,
    buildByHolder: Boolean=false)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) with SupportsLucenePushDownFilters with SupportsPushDownAggregates{
  private val partitionSchema = fileIndex.partitionSchema
  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }
  override protected val supportsNestedSchemaPruning: Boolean = false
  override def build(): Scan = {
    if (pushedAggregations.isEmpty) {
      finalSchema = readDataSchema()
    }
    LuceneScan(sparkSession, hadoopConf, fileIndex, dataSchema,
      finalSchema, readPartitionSchema(), options,  pushedFilters(),pushedAggregations,buildByHolder=buildByHolder)
  }
  protected var dataFilters = Seq.empty[Expression]
  protected var pushedDataFilters = Array.empty[Filter]

  private var _pushedFilters:Array[Filter]=Array.empty

  private var finalSchema = new StructType()

  private var pushedAggregations = Option.empty[Aggregation]

  override def pushFilters(filters: Array[Filter]): Array[Filter] ={
    _pushedFilters=filters
    Array[Filter]()
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  /**
   * Pushes down Aggregation to datasource. The order of the datasource scan output columns should
   * be: grouping columns, aggregate columns (in the same order as the aggregate functions in
   * the given Aggregation).
   *
   * @param aggregation Aggregation in SQL statement.
   * @return true if the aggregation can be pushed down to datasource, false otherwise.
   */
  override def pushAggregation(aggregation: Aggregation): Boolean = {

    AggregatePushDownUtils.getSchemaForPushedAggregation(
      aggregation,
      schema,
      partitionNameSet) match {

      case Some(schema) =>
        finalSchema = schema
        this.pushedAggregations = Some(aggregation)
        true
      case _ => false
    }
  }

  val partitionNameSet: Set[String] =
    partitionSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet
}


