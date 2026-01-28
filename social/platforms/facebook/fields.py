"""
Facebook Ads API field definitions.
"""

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.adimage import AdImage
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.customaudience import CustomAudience
from facebook_business.adobjects.customconversion import CustomConversion

fields_account_info = [
    AdAccount.Field.account_id,
    AdAccount.Field.name,
    AdAccount.Field.account_status,
    AdAccount.Field.age,
    AdAccount.Field.currency,
]

fields_custom_convers = [
    CustomConversion.Field.id,
    CustomConversion.Field.custom_event_type,
    CustomConversion.Field.rule,
]

fields_ads_insight = [
    AdsInsights.Field.account_id,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.adset_id,
    AdsInsights.Field.ad_id,
    AdsInsights.Field.ad_name,
    AdsInsights.Field.spend,
    AdsInsights.Field.impressions,
    AdsInsights.Field.reach,
    AdsInsights.Field.inline_link_clicks,
    AdsInsights.Field.inline_link_click_ctr,
    AdsInsights.Field.clicks,
    AdsInsights.Field.ctr,
    AdsInsights.Field.cpc,
    AdsInsights.Field.cpm,
]

fields_ads_insight_actions = [
    AdsInsights.Field.ad_id,
    AdsInsights.Field.actions,
]

fields_ads_campaign = [
    Campaign.Field.id,
    Campaign.Field.status,
    Campaign.Field.configured_status,
    Campaign.Field.effective_status,
    Campaign.Field.created_time,
    Campaign.Field.objective,
]

fields_ads_adset = [
    AdSet.Field.id,
    AdSet.Field.campaign_id,
    AdSet.Field.start_time,
    AdSet.Field.end_time,
    AdSet.Field.destination_type,
]

fields_ads_audience_adset = [
    AdSet.Field.id,
    AdSet.Field.name,
    AdSet.Field.campaign_id,
    AdSet.Field.targeting,
]

fields_ads_creative = [
    AdCreative.Field.account_id,
    AdCreative.Field.id,
    AdCreative.Field.actor_id,
    AdCreative.Field.name,
    AdCreative.Field.body,
]

fields_ads_images = [
    AdImage.Field.account_id,
    AdImage.Field.id,
    AdImage.Field.creatives,
    AdImage.Field.created_time,
    AdImage.Field.permalink_url,
]

fields_ads_custom_audience = [
    CustomAudience.Field.id,
    CustomAudience.Field.rule,
    CustomAudience.Field.name,
    CustomAudience.Field.associated_audience_id,
    CustomAudience.Field.data_source,
    CustomAudience.Field.creation_params,
    CustomAudience.Field.description,
    CustomAudience.Field.account_id,
    CustomAudience.Field.pixel_id,
]
