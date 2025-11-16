package pipeline

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

object ETLPipeline {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HierarchicalAccountsPipeline")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    try {
      val config = ConfigFactory.load()
      val pipeline = new HierarchicalAccountsPipeline(spark, config)
      
      println("Starting Hierarchical Accounts ETL Pipeline...")
      val result = pipeline.executeFullPipeline()
      
      println(s"Pipeline completed successfully. Processed ${result.count()} records.")
      
    } catch {
      case ex: Exception =>
        println(s"Pipeline failed with error: ${ex.getMessage}")
        ex.printStackTrace()
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}

class HierarchicalAccountsPipeline(spark: SparkSession, config: com.typesafe.config.Config) {
  
  private val jdbcUrl = config.getString("database.url")
  private val dbUser = config.getString("database.user") 
  private val dbPassword = config.getString("database.password")
  
  private val connectionProps = new java.util.Properties()
  connectionProps.setProperty("user", dbUser)
  connectionProps.setProperty("password", dbPassword)
  connectionProps.setProperty("driver", "org.postgresql.Driver")
  
  // Initialize pipeline components
  private val extractor = new DataExtractor(spark)
  private val normalizer = new DataNormalizer(spark)
  private val hierarchyBuilder = new HierarchyBuilder(spark)
  private val temporalProcessor = new TemporalProcessor(spark)
  private val outputGenerator = new OutputGenerator(spark)
  
  def executeFullPipeline(): Dataset[TemporalSegment] = {
    
    // Stage 1: Data Extraction
    println("Stage 1: Extracting source data...")
    val rawRelationships = extractor.extractAccountRelationships()
    val rawPartnerAccounts = extractor.extractPartnerAccounts()
    val rawAccountMappings = extractor.extractAccountMappings()
    
    // Stage 2: Data Normalization
    println("Stage 2: Normalizing and cleaning data...")
    val (normalizedRelationships, entityTypes, cleanMappings) = 
      normalizer.executeNormalization(rawRelationships, rawPartnerAccounts, rawAccountMappings)
    
    normalizer.validateDataQuality(normalizedRelationships)
    
    // Stage 3: Hierarchy Building
    println("Stage 3: Building account hierarchy...")
    val hierarchy = hierarchyBuilder.buildHierarchy(normalizedRelationships, entityTypes)
    hierarchyBuilder.validateHierarchy(hierarchy)
    
    // Stage 4: Temporal Processing
    println("Stage 4: Processing temporal inheritance...")
    val (hierarchySegments, standaloneSegments) = 
      temporalProcessor.executeTemporalProcessing(hierarchy, cleanMappings, normalizedRelationships, entityTypes)
    
    temporalProcessor.validateTemporalLogic(hierarchySegments)
    
    // Stage 5: Output Generation
    println("Stage 5: Generating final output...")
    val finalResult = outputGenerator.executeOutputGeneration(
      hierarchySegments, standaloneSegments, entityTypes, jdbcUrl, connectionProps)
    
    outputGenerator.validateOutput(finalResult)
    
    finalResult
  }
  
  // Alternative execution with custom parameters
  def executeWithParameters(
    maxDepth: Int = 12,
    enableValidation: Boolean = true,
    persistIntermediateResults: Boolean = false
  ): Dataset[TemporalSegment] = {
    
    // Implementation would pass parameters to individual components
    executeFullPipeline()
  }
}
