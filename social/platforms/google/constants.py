"""Google Ads Platform Constants.

This module contains all constant definitions for the Google Ads platform,
including account mappings, GAQL query templates, and configuration values.

Design:
- Centralized constant management
- Type-safe definitions
- Clear separation of concerns
- Production-ready configuration
"""

from typing import Dict

# ============================================================================
# Account-to-Company Mapping
# ============================================================================

COMPANY_ACCOUNT_MAP: Dict[str, int] = {
    "9474097201": 1,  # Main Google Ads account
    "4619434319": 1,  # Secondary Google Ads account
}

# ============================================================================
# API Configuration
# ============================================================================

API_VERSION: str = "v18"
DEFAULT_LOOKBACK_DAYS: int = 150
MICROS_DIVISOR: int = 1_000_000  # Google Ads costs are in micros (1/1,000,000 of currency)

# ============================================================================
# GAQL Query Templates
# ============================================================================

GAQL_QUERIES: Dict[str, str] = {
    # Customer hierarchy query - get all accounts under MCC
    "query_customer_hierarchy": """
        SELECT
          customer_client.client_customer,
          customer_client.level,
          customer_client.manager,
          customer_client.descriptive_name,
          customer_client.currency_code,
          customer_client.time_zone,
          customer_client.id,
          customer_client.status
        FROM customer_client
    """,

    # Campaign query - basic campaign information
    "query_campaign": """
        SELECT campaign.start_date,
        campaign.end_date, campaign.name,
        campaign.id, campaign.serving_status,
        customer.id, campaign.status
        FROM campaign
        WHERE segments.date BETWEEN '{}' AND '{}'
    """,

    # Ad report query - performance metrics (with date filter for daily data)
    "query_ad_report": """
        SELECT metrics.clicks, metrics.conversions,
        metrics.average_cpc, metrics.average_cost,
        metrics.average_cpm, metrics.impressions, metrics.cost_micros,
        ad_group_ad.ad.id, ad_group.id, campaign.id,
        metrics.ctr, segments.date, customer.id
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{}' AND '{}'
    """,

    # Ad report query - lifetime aggregated metrics (no date filter)
    "query_ad_report_lifetime": """
        SELECT metrics.clicks, metrics.conversions,
        metrics.average_cpc, metrics.average_cost,
        metrics.average_cpm, metrics.impressions, metrics.cost_micros,
        ad_group_ad.ad.id, ad_group.id, campaign.id,
        metrics.ctr, customer.id
        FROM ad_group_ad
    """,

    # Ad creatives query - creative details
    "query_ads_ad_creatives": """
        SELECT ad_group_ad.ad.type,
        ad_group_ad.ad.name,
        ad_group_ad.ad.id,
        ad_group.id,
        customer.id FROM ad_group_ad
    """,

    # Placement query - for ENDED/SERVING campaigns (matches old production)
    "query_placement": """
        SELECT group_placement_view.placement,
        group_placement_view.placement_type,
        group_placement_view.display_name,
        group_placement_view.target_url,
        ad_group.id,
        metrics.impressions,
        metrics.active_view_ctr,
        customer.id
        FROM group_placement_view
        WHERE campaign.serving_status IN ('ENDED','SERVING')
        AND segments.date BETWEEN '{}' AND '{}'
        ORDER BY metrics.impressions DESC
    """,

    # Placement query - for PAUSED campaigns
    "query_placement_2": """
        SELECT group_placement_view.placement,
        group_placement_view.placement_type,
        group_placement_view.display_name,
        group_placement_view.target_url,
        ad_group.id,
        metrics.impressions,
        metrics.active_view_ctr,
        customer.id
        FROM group_placement_view
        WHERE campaign.status IN ('PAUSED')
        AND segments.date BETWEEN '{}' AND '{}'
        ORDER BY metrics.impressions DESC
    """,

    # Audience query - for ENABLED campaigns
    "query_audience": """
        SELECT
          ad_group.id,
          ad_group_criterion.display_name,
          customer.id
        FROM ad_group_audience_view
        WHERE campaign.serving_status IN ('ENDED','SERVING')
    """,

    # Audience query - for PAUSED campaigns
    "query_audience_2": """
        SELECT
          ad_group.id,
          ad_group_criterion.display_name,
          customer.id
        FROM ad_group_audience_view
        WHERE campaign.status IN ('PAUSED')
    """,

    # Device breakdown query - for ENABLED campaigns
    "query_by_device": """
        SELECT ad_group_ad.ad.id, metrics.cost_micros,
        metrics.clicks, segments.device,
        customer.id FROM ad_group_ad
        WHERE campaign.serving_status IN ('ENDED','SERVING')
    """,

    # Device breakdown query - for PAUSED campaigns
    "query_by_device_2": """
        SELECT ad_group_ad.ad.id, metrics.cost_micros,
        metrics.clicks, segments.device,
        customer.id FROM ad_group_ad
        WHERE campaign.status IN ('PAUSED')
    """,

    # Policy violation query - ads with approval issues
    "query_violation_policy": """
        SELECT
          ad_group_ad.ad.id,
          ad_group_ad.policy_summary.approval_status,
          ad_group_ad.policy_summary.review_status,
          campaign.id,
          campaign.status,
          customer.id
        FROM ad_group_ad
        WHERE ad_group_ad.policy_summary.approval_status = 'DISAPPROVED'
        OR ad_group_ad.policy_summary.approval_status = 'AREA_OF_INTEREST_ONLY'
    """,
}

# ============================================================================
# Column Mappings (for renaming)
# ============================================================================

COLUMN_MAPPINGS: Dict[str, str] = {
    # Customer fields
    "customer.id": "customer_id_google",
    "customer_id": "customer_id_google",

    # Campaign fields
    "campaign.id": "campaign_id",
    "campaign.name": "campaign_name",
    "campaign.status": "status",
    "campaign.start_date": "start_date",
    "campaign.end_date": "end_date",
    "campaign.serving_status": "serving_status",

    # Ad Group fields
    "ad_group.id": "adgroup_id",
    "adGroup.id": "adgroup_id",
    "id": "id",  # Keep 'id' as 'id' for placement table (after handle_columns, adGroup.id becomes 'id')

    # Ad fields
    "ad_group_ad.ad.id": "ad_id",
    "ad.id": "ad_id",
    "ad_group_ad.ad.name": "ad_name",
    "ad.name": "ad_name",
    "ad_group_ad.ad.type": "ad_type",

    # Metrics fields
    "metrics.clicks": "clicks",
    "metrics.impressions": "impressions",
    "metrics.conversions": "conversions",
    "metrics.cost_micros": "cost_micros",
    "metrics.average_cpc": "average_cpc",
    "metrics.average_cpm": "average_cpm",
    "metrics.average_cost": "average_cost",
    "metrics.ctr": "ctr",
    "metrics.active_view_ctr": "active_view_ctr",

    # Segment fields
    "segments.date": "date",
    "segments.device": "device",

    # Placement fields
    "group_placement_view.resource_name": "resource_name",
    "group_placement_view.placement": "placement",
    "group_placement_view.placement_type": "placement_type",
    "group_placement_view.display_name": "display_name",
    "group_placement_view.target_url": "target_url",

    # Criterion fields
    "ad_group_criterion.display_name": "display_name",

    # Customer client fields (hierarchy)
    "customer_client.id": "id",
    "customer_client.descriptive_name": "descriptive_name",
    "customer_client.manager": "manager",
    "customer_client.status": "status",
    "customer_client.currency_code": "currency_code",
    "customer_client.time_zone": "time_zone",
    "customer_client.level": "level",
    "customer_client.client_customer": "client_customer",

    # Customer client fields (camelCase from MessageToDict)
    "descriptiveName": "descriptive_name",
    "clientCustomer": "client_customer",
    "timeZone": "time_zone",
    "currencyCode": "currency_code",
    "managerId": "manager_id",

    # Ad group criterion fields (camelCase from json_normalize)
    "adGroupCriterion.displayName": "display_name",

    # After handle_columns() removes prefixes and converts to lowercase
    # "adGroupCriterion.displayName" → "displayName" → "displayname"
    "displayName": "display_name",
    "displayname": "display_name",

    # Campaign fields after handle_columns() lowercase conversion
    "startdate": "start_date",
    "enddate": "end_date",
    "servingstatus": "serving_status",

    # Metrics fields after handle_columns() lowercase conversion
    "costmicros": "cost_micros",
    "activeviewctr": "active_view_ctr",

    # Placement fields after handle_columns() lowercase conversion
    "placementtype": "placement_type",
    "targeturl": "target_url",
}
