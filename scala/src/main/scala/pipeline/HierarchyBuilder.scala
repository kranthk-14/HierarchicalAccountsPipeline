package pipeline

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

class HierarchyBuilder(spark: SparkSession) {
  
  import spark.implicits._
  
  def buildHierarchy(relationships: Dataset[AccountRelationship], entityTypes: Dataset[EntityType]): Dataset[HierarchyNode] = {
    
    // Start with base level relationships
    var currentLevel = relationships
      .select(
        $"parentAccountId",
        $"childAccountId", 
        $"parentAccountId".alias("rootAccountId"),
        lit(1).alias("levelDepth"),
        concat($"parentAccountId", lit("/"), $"childAccountId").alias("pathTrace")
      )
    
    var allHierarchy = currentLevel
    var maxDepth = 12
    var currentDepth = 1
    
    // Iteratively build deeper levels (replaces SQL recursive CTE)
    while (currentDepth < maxDepth && !currentLevel.isEmpty) {
      currentDepth += 1
      
      val nextLevel = relationships.alias("rel")
        .join(currentLevel.alias("hier"), $"rel.parentAccountId" === $"hier.childAccountId")
        .filter(
          // Cycle prevention logic
          !$"hier.pathTrace".contains(concat(lit("/"), $"rel.childAccountId", lit("/"))) &&
          !$"hier.pathTrace".startsWith(concat($"rel.childAccountId", lit("/"))) &&
          $"hier.pathTrace" =!= $"rel.childAccountId"
        )
        .select(
          $"rel.parentAccountId",
          $"rel.childAccountId",
          $"hier.rootAccountId",
          lit(currentDepth).alias("levelDepth"),
          concat($"hier.pathTrace", lit("/"), $"rel.childAccountId").alias("pathTrace")
        )
      
      if (!nextLevel.isEmpty) {
        allHierarchy = allHierarchy.union(nextLevel)
        currentLevel = nextLevel
      } else {
        currentLevel = spark.emptyDataFrame.select(
          lit("").alias("parentAccountId"),
          lit("").alias("childAccountId"),
          lit("").alias("rootAccountId"),
          lit(0).alias("levelDepth"),
          lit("").alias("pathTrace")
        ).limit(0)
      }
    }
    
    // Add partner flags
    val hierarchyWithPartnerFlags = addPartnerFlags(allHierarchy, entityTypes)
    
    // Add root detection logic
    addRootDetection(hierarchyWithPartnerFlags)
  }
  
  private def addPartnerFlags(hierarchy: Dataset[_], entityTypes: Dataset[EntityType]): Dataset[_] = {
    val partnerEntities = entityTypes.filter($"entityType" === "PARTNER")
    
    hierarchy
      .join(
        partnerEntities.select($"accountId".alias("parentAccountId")).withColumn("parentIsPartner", lit(1)),
        Seq("parentAccountId"), 
        "left"
      )
      .join(
        partnerEntities.select($"accountId".alias("childAccountId")).withColumn("childIsPartner", lit(1)),
        Seq("childAccountId"), 
        "left"
      )
      .join(
        partnerEntities.select($"accountId".alias("rootAccountId")).withColumn("rootIsPartner", lit(1)),
        Seq("rootAccountId"), 
        "left"
      )
      .na.fill(0, Seq("parentIsPartner", "childIsPartner", "rootIsPartner"))
  }
  
  private def addRootDetection(hierarchyWithFlags: Dataset[_]): Dataset[HierarchyNode] = {
    hierarchyWithFlags
      .withColumn("isRootManager",
        when($"parentAccountId" === $"rootAccountId", 1).otherwise(0)
      )
      .withColumn("isRootPartnerManager",
        when($"parentIsPartner" === 1 && $"levelDepth" === 1, 1)
          .when($"rootIsPartner" === 1 && $"parentAccountId" === $"rootAccountId", 1)
          .otherwise(0)
      )
      .as[HierarchyNode]
  }
  
  // Alternative graph-based approach using GraphX (more efficient for very large datasets)
  def buildHierarchyGraphX(relationships: Dataset[AccountRelationship]): Dataset[HierarchyNode] = {
    // This would use Spark GraphX for graph algorithms
    // Implementation would involve:
    // 1. Create graph from relationships
    // 2. Find connected components
    // 3. Detect cycles using graph algorithms
    // 4. Calculate shortest paths for hierarchy levels
    
    // For now, return regular implementation
    buildHierarchy(relationships, spark.emptyDataset[EntityType])
  }
  
  // Validation methods
  def validateHierarchy(hierarchy: Dataset[HierarchyNode]): Unit = {
    val totalNodes = hierarchy.count()
    val maxDepth = hierarchy.agg(max($"levelDepth")).collect()(0)(0)
    val rootCount = hierarchy.filter($"isRootManager" === 1).count()
    val cycleCheck = hierarchy.filter($"pathTrace".contains($"childAccountId")).count()
    
    println(s"Hierarchy Validation:")
    println(s"Total nodes: $totalNodes")
    println(s"Maximum depth: $maxDepth")
    println(s"Root managers: $rootCount")
    println(s"Potential cycles: $cycleCheck")
    
    if (cycleCheck > 0) {
      println("WARNING: Potential cycles detected in hierarchy")
    }
  }
}
