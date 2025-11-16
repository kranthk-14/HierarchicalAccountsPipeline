package pipeline

import java.sql.{Date, Timestamp}

case class AccountRelationship(
  parentAccountId: String,
  childAccountId: String,
  createdAt: Timestamp,
  updatedAt: Timestamp,
  isDeleted: Int
)

case class PartnerAccount(
  partnerAccountId: String,
  groupAccountId: String,
  isApproved: Int,
  isExternal: Int,
  isDisabled: Int
)

case class AccountMapping(
  managerAccountId: String,
  entityId: String,
  marketplaceId: Int,
  effectiveStartDate: Date,
  effectiveEndDate: Date,
  source: String,
  advertiserId: String,
  isLinked: Int
)

case class EntityType(
  accountId: String,
  entityType: String,
  isApproved: Int,
  isExternal: Int
)

case class HierarchyNode(
  parentAccountId: String,
  childAccountId: String,
  rootAccountId: String,
  levelDepth: Int,
  pathTrace: String,
  parentIsPartner: Int = 0,
  childIsPartner: Int = 0,
  rootIsPartner: Int = 0,
  isRootManager: Int = 0,
  isRootPartnerManager: Int = 0
)

case class TemporalSegment(
  accountId: String,
  entityId: Option[String],
  marketplaceId: Option[Int],
  effectiveStartDate: Date,
  effectiveEndDate: Date,
  levelDepth: Int,
  rootAccountId: String,
  isRootManager: Int,
  isRootPartnerManager: Int,
  source: String,
  recordType: String
)

case class DateBreakpoint(
  periodStart: Date,
  periodEnd: Date
)
