package pipeline

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.sql.Date

class TemporalProcessor(spark: SparkSession) {
  
  import spark.implicits._
  
  def createDateBreakpoints(cleanMappings: Dataset[AccountMapping], relationships: Dataset[AccountRelationship]): Dataset[DateBreakpoint] = {
    
    // Collect all significant dates
    val startDates = cleanMappings.select($"effectiveStartDate".alias("breakpointDate"))
    val endDates = cleanMappings
      .filter($"effectiveEndDate" =!= Date.valueOf("2099-12-31"))
      .select($"effectiveEndDate".alias("breakpointDate"))
    val createdDates = relationships.select(date_format($"createdAt", "yyyy-MM-dd").cast("date").alias("breakpointDate"))
    
    val allDates = startDates.union(endDates).union(createdDates).distinct()
    
    // Create ordered periods using window functions
    val dateWindow = Window.orderBy($"breakpointDate")
    
    allDates
      .withColumn("nextDate", lead($"breakpointDate", 1).over(dateWindow))
      .select(
        $"breakpointDate".alias("periodStart"),
        when($"nextDate".isNotNull, 
             date_sub($"nextDate", 1))
          .otherwise(Date.valueOf("2099-12-31")).alias("periodEnd")
      )
      .filter($"periodStart".isNotNull)
      .orderBy($"periodStart")
      .as[DateBreakpoint]
  }
  
  def createTemporalSegments(
    hierarchy: Dataset[HierarchyNode], 
    breakpoints: Dataset[DateBreakpoint],
    cleanMappings: Dataset[AccountMapping]
  ): Dataset[TemporalSegment] = {
    
    // Cross join hierarchy with date breakpoints
    val segments = hierarchy
      .crossJoin(breakpoints)
      .join(
        cleanMappings,
        hierarchy("parentAccountId") === cleanMappings("managerAccountId") &&
        breakpoints("periodStart") >= cleanMappings("effectiveStartDate") &&
        breakpoints("periodEnd") <= cleanMappings("effectiveEndDate"),
        "left"
      )
      .select(
        hierarchy("parentAccountId").alias("accountId"),
        cleanMappings("entityId"),
        cleanMappings("marketplaceId"),
        breakpoints("periodStart").alias("effectiveStartDate"),
        breakpoints("periodEnd").alias("effectiveEndDate"),
        hierarchy("levelDepth"),
        hierarchy("rootAccountId"),
        hierarchy("isRootManager"),
        hierarchy("isRootPartnerManager"),
        coalesce(cleanMappings("source"), lit("HIERARCHY")).alias("source"),
        lit("HIERARCHY").alias("recordType")
      )
      .as[TemporalSegment]
    
    segments
  }
  
  def processInheritanceLogic(
    segments: Dataset[TemporalSegment], 
    entityTypes: Dataset[EntityType]
  ): Dataset[TemporalSegment] = {
    
    segments
      .withColumn("calculatedRootFlag",
        when($"entityId".isNotNull, $"isRootManager")
          .when($"entityId".isNull, 1)  // When no mapping exists, assume root
          .otherwise(0)
      )
      .withColumn("calculatedPartnerFlag",
        when($"entityId".isNotNull, $"isRootPartnerManager")
          .when($"entityId".isNull, 1)  // When no mapping exists for partner, assume root partner
          .otherwise(0)
      )
      .select(
        $"accountId",
        $"entityId",
        $"marketplaceId",
        $"effectiveStartDate",
        $"effectiveEndDate",
        $"levelDepth",
        $"rootAccountId",
        $"calculatedRootFlag".alias("isRootManager"),
        $"calculatedPartnerFlag".alias("isRootPartnerManager"),
        $"source",
        $"recordType"
      )
      .as[TemporalSegment]
  }
  
  def createPostHierarchyInheritance(
    segments: Dataset[TemporalSegment],
    entityTypes: Dataset[EntityType]
  ): Dataset[TemporalSegment] = {
    
    // Find hierarchy end dates for each child account
    val hierarchyEnds = segments
      .filter($"entityId".isNotNull)
      .groupBy($"accountId")
      .agg(max($"effectiveEndDate").alias("hierarchyEndDate"))
      .filter($"hierarchyEndDate" < Date.valueOf("2099-12-31"))
    
    // Create standalone inheritance periods
    val partnerEntities = entityTypes.filter($"entityType" === "PARTNER")
    
    hierarchyEnds
      .join(partnerEntities, hierarchyEnds("accountId") === partnerEntities("accountId"))
      .select(
        hierarchyEnds("accountId"),
        lit(null).cast("string").alias("entityId"),
        lit(null).cast("int").alias("marketplaceId"),
        date_add($"hierarchyEndDate", 1).alias("effectiveStartDate"),
        lit(Date.valueOf("2099-12-31")).alias("effectiveEndDate"),
        lit(0).alias("levelDepth"),
        hierarchyEnds("accountId").alias("rootAccountId"),
        lit(1).alias("isRootManager"),
        lit(1).alias("isRootPartnerManager"),
        lit("INHERITANCE").alias("source"),
        lit("STANDALONE").alias("recordType")
      )
      .as[TemporalSegment]
  }
  
  // Complete temporal processing pipeline
  def executeTemporalProcessing(
    hierarchy: Dataset[HierarchyNode],
    cleanMappings: Dataset[AccountMapping],
    relationships: Dataset[AccountRelationship],
    entityTypes: Dataset[EntityType]
  ): (Dataset[TemporalSegment], Dataset[TemporalSegment]) = {
    
    val breakpoints = createDateBreakpoints(cleanMappings, relationships)
    val segments = createTemporalSegments(hierarchy, breakpoints, cleanMappings)
    val processedSegments = processInheritanceLogic(segments, entityTypes)
    val postHierarchyInheritance = createPostHierarchyInheritance(processedSegments, entityTypes)
    
    // Cache results
    processedSegments.cache()
    postHierarchyInheritance.cache()
    
    (processedSegments, postHierarchyInheritance)
  }
  
  // Validation methods
  def validateTemporalLogic(segments: Dataset[TemporalSegment]): Unit = {
    val totalSegments = segments.count()
    val dateOverlaps = segments
      .groupBy($"accountId", $"entityId")
      .agg(
        min($"effectiveStartDate").alias("minStart"),
        max($"effectiveEndDate").alias("maxEnd"),
        count("*").alias("segmentCount")
      )
      .filter($"segmentCount" > 1)
      .count()
    
    println(s"Temporal Validation:")
    println(s"Total segments: $totalSegments")
    println(s"Accounts with multiple segments: $dateOverlaps")
  }
}
