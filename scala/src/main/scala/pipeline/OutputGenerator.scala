package pipeline

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.sql.{Date, Timestamp}

class OutputGenerator(spark: SparkSession) {
  
  import spark.implicits._
  
  def consolidateFinalData(
    hierarchySegments: Dataset[TemporalSegment],
    standaloneSegments: Dataset[TemporalSegment]
  ): Dataset[TemporalSegment] = {
    
    // Union hierarchy and standalone data
    val combinedData = hierarchySegments.union(standaloneSegments)
    
    // Final deduplication with business rule prioritization
    val deduplicationWindow = Window
      .partitionBy($"accountId", $"entityId", $"marketplaceId", $"effectiveStartDate", $"effectiveEndDate")
      .orderBy(
        $"isRootPartnerManager".desc,
        when($"source" === "API", 1)
          .when($"source" === "INHERITANCE", 2)
          .when($"source" === "MANUAL", 3)
          .otherwise(4),
        $"levelDepth".asc
      )
    
    combinedData
      .withColumn("finalRank", row_number().over(deduplicationWindow))
      .filter($"finalRank" === 1)
      .drop("finalRank")
      .as[TemporalSegment]
  }
  
  def createFactTable(consolidatedData: Dataset[TemporalSegment]): Dataset[TemporalSegment] = {
    val rowNumberWindow = Window.orderBy($"accountId", $"effectiveStartDate")
    
    consolidatedData
      .withColumn("hierarchyKey", row_number().over(rowNumberWindow))
      .withColumn("createdAt", current_timestamp())
      .as[TemporalSegment]
  }
  
  def createDimensionAccounts(
    consolidatedData: Dataset[TemporalSegment], 
    entityTypes: Dataset[EntityType]
  ): Dataset[EntityType] = {
    
    consolidatedData
      .select($"accountId")
      .distinct()
      .join(entityTypes, Seq("accountId"), "left")
      .withColumn("createdAt", current_timestamp())
      .na.fill("UNKNOWN", Seq("entityType"))
      .na.fill(0, Seq("isApproved", "isExternal"))
      .as[EntityType]
  }
  
  def createDimensionDate(consolidatedData: Dataset[TemporalSegment]): Dataset[(Date, Int, Int, Int, Int)] = {
    val startDates = consolidatedData.select($"effectiveStartDate".alias("dateKey"))
    val endDates = consolidatedData.select($"effectiveEndDate".alias("dateKey"))
    
    startDates
      .union(endDates)
      .distinct()
      .filter($"dateKey".isNotNull)
      .select(
        $"dateKey",
        year($"dateKey").alias("yearNum"),
        month($"dateKey").alias("monthNum"),
        dayofmonth($"dateKey").alias("dayNum"),
        quarter($"dateKey").alias("quarterNum")
      )
      .as[(Date, Int, Int, Int, Int)]
  }
  
  // Write final tables to database
  def writeToDatabase(
    factData: Dataset[TemporalSegment],
    dimAccounts: Dataset[EntityType],
    dimDate: Dataset[(Date, Int, Int, Int, Int)],
    jdbcUrl: String,
    connectionProps: java.util.Properties
  ): Unit = {
    
    println("Writing fact_account_hierarchy table...")
    factData
      .select(
        $"hierarchyKey",
        $"accountId",
        $"entityId", 
        $"marketplaceId",
        $"rootAccountId",
        $"levelDepth",
        $"effectiveStartDate",
        $"effectiveEndDate",
        $"isRootManager",
        $"isRootPartnerManager",
        $"source",
        $"recordType",
        $"createdAt"
      )
      .write
      .mode("overwrite")
      .jdbc(jdbcUrl, "fact_account_hierarchy", connectionProps)
    
    println("Writing dim_accounts table...")
    dimAccounts
      .write
      .mode("overwrite") 
      .jdbc(jdbcUrl, "dim_accounts", connectionProps)
    
    println("Writing dim_date table...")
    dimDate
      .toDF("date_key", "year_num", "month_num", "day_num", "quarter_num")
      .write
      .mode("overwrite")
      .jdbc(jdbcUrl, "dim_date", connectionProps)
  }
  
  // Complete output generation pipeline
  def executeOutputGeneration(
    hierarchySegments: Dataset[TemporalSegment],
    standaloneSegments: Dataset[TemporalSegment],
    entityTypes: Dataset[EntityType],
    jdbcUrl: String,
    connectionProps: java.util.Properties
  ): Dataset[TemporalSegment] = {
    
    val consolidatedData = consolidateFinalData(hierarchySegments, standaloneSegments)
    val factData = createFactTable(consolidatedData)
    val dimAccounts = createDimensionAccounts(consolidatedData, entityTypes)
    val dimDate = createDimensionDate(consolidatedData)
    
    // Write to database
    writeToDatabase(factData, dimAccounts, dimDate, jdbcUrl, connectionProps)
    
    // Return fact data for further processing if needed
    factData.cache()
    factData
  }
  
  // Performance optimization methods
  def optimizeForPerformance(data: Dataset[TemporalSegment]): Dataset[TemporalSegment] = {
    data
      .repartition($"accountId")  // Partition by account for better join performance
      .cache()
  }
  
  // Data quality validation
  def validateOutput(factData: Dataset[TemporalSegment]): Unit = {
    val totalRecords = factData.count()
    val duplicateCheck = factData
      .groupBy($"accountId", $"entityId", $"effectiveStartDate", $"effectiveEndDate")
      .count()
      .filter($"count" > 1)
      .count()
    
    val dateConsistency = factData
      .filter($"effectiveStartDate" > $"effectiveEndDate")
      .count()
    
    println(s"Output Validation:")
    println(s"Total records: $totalRecords")
    println(s"Duplicates: $duplicateCheck")
    println(s"Date inconsistencies: $dateConsistency")
    
    if (duplicateCheck > 0 || dateConsistency > 0) {
      println("WARNING: Data quality issues in final output")
    }
  }
}
