package pipeline

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class DataNormalizer(spark: SparkSession) {
  
  import spark.implicits._
  
  def normalizeRelationships(relationships: Dataset[AccountRelationship]): Dataset[AccountRelationship] = {
    val deduplicationWindow = Window
      .partitionBy($"parentAccountId", $"childAccountId")
      .orderBy($"updatedAt".desc, $"createdAt".desc)
    
    relationships
      .filter($"parentAccountId" =!= $"childAccountId")
      .withColumn("rn", row_number().over(deduplicationWindow))
      .filter($"rn" === 1)
      .filter(
        $"parentAccountId".isNotNull &&
        $"childAccountId".isNotNull &&
        length($"parentAccountId") > 0 &&
        length($"childAccountId") > 0
      )
      .drop("rn")
      .as[AccountRelationship]
  }
  
  def createEntityTypes(
    partnerAccounts: Dataset[PartnerAccount],
    accountMappings: Dataset[AccountMapping]
  ): Dataset[EntityType] = {
    
    val partnerEntities = partnerAccounts
      .select(
        $"partnerAccountId".alias("accountId"),
        lit("PARTNER").alias("entityType"),
        $"isApproved",
        $"isExternal"
      )
    
    val managerEntities = accountMappings
      .select($"managerAccountId".alias("accountId"))
      .distinct()
      .select(
        $"accountId",
        lit("MANAGER").alias("entityType"),
        lit(1).alias("isApproved"),
        lit(0).alias("isExternal")
      )
    
    partnerEntities
      .union(managerEntities)
      .distinct()
      .as[EntityType]
  }
  
  def cleanAccountMappings(mappings: Dataset[AccountMapping]): Dataset[AccountMapping] = {
    val priorityWindow = Window
      .partitionBy($"managerAccountId", $"entityId", $"marketplaceId", $"effectiveStartDate")
      .orderBy(
        when($"source" === "API", 1)
          .when($"source" === "MANUAL", 2)
          .otherwise(3),
        $"effectiveEndDate".desc
      )
    
    mappings
      .filter($"effectiveStartDate" <= $"effectiveEndDate")
      .withColumn("priorityRank", row_number().over(priorityWindow))
      .filter($"priorityRank" === 1)
      .drop("priorityRank")
      .as[AccountMapping]
  }
  
  // Complete normalization pipeline
  def executeNormalization(
    relationships: Dataset[AccountRelationship],
    partnerAccounts: Dataset[PartnerAccount],
    accountMappings: Dataset[AccountMapping]
  ): (Dataset[AccountRelationship], Dataset[EntityType], Dataset[AccountMapping]) = {
    
    val normalizedRelationships = normalizeRelationships(relationships)
    val entityTypes = createEntityTypes(partnerAccounts, accountMappings)
    val cleanMappings = cleanAccountMappings(accountMappings)
    
    // Cache for performance as these will be reused
    normalizedRelationships.cache()
    entityTypes.cache()
    cleanMappings.cache()
    
    (normalizedRelationships, entityTypes, cleanMappings)
  }
  
  // Data quality validation methods
  def validateDataQuality(relationships: Dataset[AccountRelationship]): Unit = {
    val totalRecords = relationships.count()
    val selfReferences = relationships.filter($"parentAccountId" === $"childAccountId").count()
    val nullParents = relationships.filter($"parentAccountId".isNull).count()
    val nullChildren = relationships.filter($"childAccountId".isNull).count()
    
    println(s"Data Quality Report:")
    println(s"Total relationships: $totalRecords")
    println(s"Self-references: $selfReferences")
    println(s"Null parents: $nullParents")
    println(s"Null children: $nullChildren")
    
    if (selfReferences > 0 || nullParents > 0 || nullChildren > 0) {
      println("WARNING: Data quality issues detected")
    }
  }
}
