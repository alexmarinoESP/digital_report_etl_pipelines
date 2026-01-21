"""
Google Ads API query definitions.
"""

# Ad creatives query
query_ads_ad_creatives = """
SELECT
    ad_group_ad.ad.id,
    ad_group_ad.ad.name,
    ad_group_ad.ad.final_urls,
    ad_group_ad.ad.display_url,
    customer.id
FROM ad_group_ad
WHERE segments.date BETWEEN '{}' AND '{}'
"""

# Cost by device query
query_by_device = """
SELECT
    ad_group_ad.ad.id,
    segments.device,
    metrics.clicks,
    metrics.cost_micros,
    customer.id
FROM ad_group_ad
WHERE segments.date BETWEEN '{}' AND '{}'
AND campaign.status = 'ENABLED'
AND ad_group.status = 'ENABLED'
AND ad_group_ad.status = 'ENABLED'
"""

query_by_device_2 = """
SELECT
    ad_group_ad.ad.id,
    segments.device,
    metrics.clicks,
    metrics.cost_micros,
    customer.id
FROM ad_group_ad
WHERE segments.date BETWEEN '{}' AND '{}'
AND campaign.status = 'PAUSED'
"""

# Campaign query
query_campaign = """
SELECT
    campaign.id,
    campaign.name,
    campaign.status,
    campaign.start_date,
    campaign.end_date,
    customer.id
FROM campaign
WHERE segments.date BETWEEN '{}' AND '{}'
"""

# Placement queries
query_placement = """
SELECT
    ad_group_ad.ad.id,
    group_placement_view.placement,
    group_placement_view.placement_type,
    group_placement_view.display_name,
    group_placement_view.target_url,
    metrics.impressions,
    metrics.active_view_ctr,
    segments.date,
    customer.id
FROM group_placement_view
WHERE segments.date BETWEEN '{}' AND '{}'
AND campaign.status = 'ENABLED'
"""

query_placement_2 = """
SELECT
    ad_group_ad.ad.id,
    group_placement_view.placement,
    group_placement_view.placement_type,
    group_placement_view.display_name,
    group_placement_view.target_url,
    metrics.impressions,
    metrics.active_view_ctr,
    segments.date,
    customer.id
FROM group_placement_view
WHERE segments.date BETWEEN '{}' AND '{}'
AND campaign.status = 'PAUSED'
"""

# Audience queries
query_audience = """
SELECT
    ad_group.id,
    ad_group_audience_view.display_name,
    customer.id
FROM ad_group_audience_view
WHERE segments.date BETWEEN '{}' AND '{}'
AND campaign.status = 'ENABLED'
"""

query_audience_2 = """
SELECT
    ad_group.id,
    ad_group_audience_view.display_name,
    customer.id
FROM ad_group_audience_view
WHERE segments.date BETWEEN '{}' AND '{}'
AND campaign.status = 'PAUSED'
"""

# Ad report query
query_ad_report = """
SELECT
    ad_group_ad.ad.id,
    ad_group_ad.ad.name,
    ad_group.id,
    campaign.id,
    metrics.impressions,
    metrics.clicks,
    metrics.cost_micros,
    segments.date,
    customer.id
FROM ad_group_ad
WHERE segments.date BETWEEN '{}' AND '{}'
"""

# Policy violation query
query_violation_policy = """
SELECT
    ad_group_ad.ad.id,
    ad_group_ad.policy_summary.approval_status,
    ad_group_ad.policy_summary.review_status,
    customer.id
FROM ad_group_ad
WHERE ad_group_ad.policy_summary.approval_status != 'APPROVED'
"""
