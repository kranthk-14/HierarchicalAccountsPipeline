package pipeline

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Date

class DataExtractor(spark: SparkSession) {
  
  import spark.implicits._
  
  def extractAccountRelationships(): Dataset[AccountRelationship] = {
    spark.sql("""
      SELECT 
        entity_id_one as parentAccountId,
        entity_id_two as childAccountId, 
        created_at as createdAt,
        updated_at as updatedAt,
        CASE WHEN delete_flag = 'Y' THEN 1 ELSE 0 END as isDeleted
      FROM raw_entity_relationships
      WHERE delete_flag != 'Y'
        AND entity_id_one IS NOT NULL
        AND entity_id_two IS NOT NULL
    """).as[AccountRelationship]
  }
  
  def extractPartnerAccounts(): Dataset[PartnerAccount] = {
    spark.sql("""
      SELECT 
        partner_account_id as partnerAccountId,
        group_account_id as groupAccountId,
        CASE WHEN is_registration_approved = 'Y' THEN 1 ELSE 0 END as isApproved,
        CASE WHEN is_external_partner_account = 'Y' THEN 1 ELSE 0 END as isExternal,
        CASE WHEN is_disabled = 'Y' THEN 1 ELSE 0 END as isDisabled
      FROM raw_partner_accounts
      WHERE is_disabled != 'Y'
    """).as[PartnerAccount]
  }
  
  def extractAccountMappings(): Dataset[AccountMapping] = {
    spark.sql("""
      SELECT 
        manager_account_id as managerAccountId,
        entity_id as entityId,
        marketplace_id as marketplaceId,
        effective_start_date as effectiveStartDate,
        COALESCE(effective_end_date, '2099-12-31') as effectiveEndDate,
        UPPER(source) as source,
        advertiser_id as advertiserId,
        CASE WHEN is_linked = 'Y' THEN 1 ELSE 0 END as isLinked
      FROM raw_partner_adv_entity_mapping
      WHERE is_linked = 'Y'
        AND effective_start_date IS NOT NULL
    """).as[AccountMapping]
  }
  
  // Alternative functional approach using Dataset transformations
  def extractAccountRelationshipsFunctional(): Dataset[AccountRelationship] = {
    val rawData = spark.table("raw_entity_relationships")
    
    rawData
      .filter($"delete_flag" =!= "Y" && $"entity_id_one".isNotNull && $"entity_id_two".isNotNull)
      .select(
        $"entity_id_one".alias("parentAccountId"),
        $"entity_id_two".alias("childAccountId"),
        $"created_at".alias("createdAt"),
        $"updated_at".alias("updatedAt"),
        when($"delete_flag" === "Y", 1).otherwise(0).alias("isDeleted")
      )
      .as[AccountRelationship]
  }
  
  def extractPartnerAccountsFunctional(): Dataset[PartnerAccount] = {
    val rawData = spark.table("raw_partner_accounts")
    
    rawData
      .filter($"is_disabled" =!= "Y")
      .select(
        $"partner_account_id".alias("partnerAccountId"),
        $"group_account_id".alias("groupAccountId"),
        when($"is_registration_approved" === "Y", 1).otherwise(0).alias("isApproved"),
        when($"is_external_partner_account" === "Y", 1).otherwise(0).alias("isExternal"),
        when($"is_disabled" === "Y", 1).otherwise(0).alias("isDisabled")
      )
      .as[PartnerAccount]
  }
  
  def extractAccountMappingsFunctional(): Dataset[AccountMapping] = {
    val rawData = spark.table("raw_partner_adv_entity_mapping")
    
    rawData
      .filter($"is_linked" === "Y" && $"effective_start_date".isNotNull)
      .select(
        $"manager_account_id".alias("managerAccountId"),
        $"entity_id".alias("entityId"),
        $"marketplace_id".alias("marketplaceId"),
        $"effective_start_date".alias("effectiveStartDate"),
        coalesce($"effective_end_date", lit(Date.valueOf("2099-12-31"))).alias("effectiveEndDate"),
        upper($"source").alias("source"),
        $"advertiser_id".alias("advertiserId"),
        when($"is_linked" === "Y", 1).otherwise(0).alias("isLinked")
      )
      .as[AccountMapping]
  }
}
