"""
SQL Queries for SMS Campaign Pipeline.

All queries use JinjaSQL templates with {{variable}} placeholders.
"""

# =============================================================================
# Extract SMS campaigns from database
# Join MAPP render table with master campaigns table
# Filter by SMS channel (SENTTOMTA_SENDOUTCHANNEL = 'SMS' UPPERCASE!)
# Parameters: {{company_id}} (1=IT, 2=ES, 32=VVIT), {{years_behind}}
# =============================================================================
QUERY_SMS_CAMPAIGNS = """
    SELECT DISTINCT
        f.MESSAGE_ID,
        f.MESSAGE_EXTERNALID AS ACTIVITY_ID,
        MAX(f.USER_ID) AS CONTACT_ID
    FROM ESPODS.ESP_ODS_MAPP_RENDER f
    JOIN ESPDDS.ESP_DCAMPAIGN_NEW d
        ON d.NEWSLETTERID = f.MESSAGE_EXTERNALID
    WHERE YEAR(CAST(f.RENDER_TIMESTAMP AS DATE)) >= YEAR(CURRENT_DATE) - {{years_behind}}
        AND d.COMPANYID = {{company_id}}
        AND f.SENTTOMTA_SENDOUTCHANNEL = 'SMS'
    GROUP BY f.MESSAGE_ID, f.MESSAGE_EXTERNALID
    ORDER BY f.MESSAGE_ID DESC
"""


# =============================================================================
# Check if campaign already exists (avoid duplicates)
# Fast primary key lookup
# Parameters: {{message_id}}
# Returns: cnt = 0 (not exists) or 1 (exists)
# =============================================================================
CHECK_CAMPAIGN_EXISTS = """
    SELECT COUNT(*) as cnt
    FROM ESPDM.ESP_DM_SMS_CAMPAIGN
    WHERE MESSAGE_ID = {{message_id}}
"""


# =============================================================================
# Insert SMS campaign with delivery statistics
# Main table for SMS analytics
# Parameters: message_id, activity_id, campaign_name, company_id, sms_text,
#            sendout_date, sent_count, delivered_count, bounced_count,
#            acceptance_rate
# Constraints: MESSAGE_ID (PK), ACTIVITY_ID (UK)
# Indexes: activity, company, sendout_date
# =============================================================================
INSERT_CAMPAIGN = """
    INSERT INTO ESPDM.ESP_DM_SMS_CAMPAIGN (
        MESSAGE_ID,
        ACTIVITY_ID,
        CAMPAIGN_NAME,
        COMPANY_ID,
        SMS_TEXT,
        SENDOUT_DATE,
        SENT_COUNT,
        DELIVERED_COUNT,
        BOUNCED_COUNT,
        ACCEPTANCE_RATE,
        LOAD_DATE
    ) VALUES (
        {{message_id}},
        {{activity_id}},
        {{campaign_name}},
        {{company_id}},
        {{sms_text}},
        {{sendout_date}},
        {{sent_count}},
        {{delivered_count}},
        {{bounced_count}},
        {{acceptance_rate}},
        CURRENT_DATE
    )
"""


# =============================================================================
# Insert Bitly link with click statistics
# One-to-many relationship with campaigns
# Parameters: message_id (FK), activity_id (denormalized for performance),
#            bitly_short_url, bitly_long_url, total_clicks
# Constraints: FK to ESP_DM_SMS_CAMPAIGN (CASCADE), UNIQUE (message_id, short_url)
# Note: ACTIVITY_ID denormalized for direct queries without joins
# =============================================================================
INSERT_LINK = """
    INSERT INTO ESPDM.ESP_DM_SMS_LINK (
        MESSAGE_ID,
        ACTIVITY_ID,
        BITLY_SHORT_URL,
        BITLY_LONG_URL,
        TOTAL_CLICKS,
        LOAD_DATE
    ) VALUES (
        {{message_id}},
        {{activity_id}},
        {{bitly_short_url}},
        {{bitly_long_url}},
        {{total_clicks}},
        CURRENT_DATE
    )
"""


# =============================================================================
# UPDATE Bitly link clicks (used for upsert - try update first)
# Parameters: message_id, bitly_short_url, total_clicks
# Returns: rowcount > 0 if updated, 0 if not found
# =============================================================================
UPDATE_LINK_CLICKS = """
    UPDATE ESPDM.ESP_DM_SMS_LINK
    SET TOTAL_CLICKS = {{total_clicks}},
        LOAD_DATE = CURRENT_DATE
    WHERE MESSAGE_ID = {{message_id}}
      AND BITLY_SHORT_URL = {{bitly_short_url}}
"""


# =============================================================================
# CHECK if link exists (for upsert logic)
# Parameters: message_id, bitly_short_url
# Returns: cnt = 0 (not exists) or 1 (exists)
# =============================================================================
CHECK_LINK_EXISTS = """
    SELECT COUNT(*) as cnt
    FROM ESPDM.ESP_DM_SMS_LINK
    WHERE MESSAGE_ID = {{message_id}}
      AND BITLY_SHORT_URL = {{bitly_short_url}}
"""


# =============================================================================
# Get existing links for a campaign
# Used to refresh click counts for already processed campaigns
# Parameters: {{message_id}}
# Returns: bitly_short_url, bitly_long_url, total_clicks (current values)
# =============================================================================
GET_LINKS_BY_CAMPAIGN = """
    SELECT
        BITLY_SHORT_URL,
        BITLY_LONG_URL,
        TOTAL_CLICKS
    FROM ESPDM.ESP_DM_SMS_LINK
    WHERE MESSAGE_ID = {{message_id}}
"""


# =============================================================================
# Get campaigns with links for click refresh
# Used by nightly job to update click counts for recent campaigns
# Parameters: {{days_back}} - how many days back to look (e.g., 90)
# Returns: message_id, activity_id for campaigns that need click refresh
# =============================================================================
GET_CAMPAIGNS_FOR_CLICK_REFRESH = """
    SELECT DISTINCT
        c.MESSAGE_ID,
        c.ACTIVITY_ID
    FROM ESPDM.ESP_DM_SMS_CAMPAIGN c
    JOIN ESPDM.ESP_DM_SMS_LINK l ON l.MESSAGE_ID = c.MESSAGE_ID
    WHERE c.SENDOUT_DATE >= CURRENT_DATE - {{days_back}}
    ORDER BY c.MESSAGE_ID DESC
"""
