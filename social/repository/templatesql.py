"""
SQL templates for social data operations.
Uses JinjaSQL for parameterized queries.
"""

# Where clause for update query
where_clause_jinja = """
  {% for field in fields %}
    {% if loop.first %}
      WHERE {{ field }} = :id_value_{{ field }}
    {% else %}
      AND {{ field }} = :id_value_{{ field }}
    {% endif %}
  {% endfor %}"""

# On clause for merge query
on_clause_jinja = """
  {% for field in fields %}
    {% if loop.first %}
      ON TGT.{{ field }}=SRC.{{ field }}
    {% else %}
      AND TGT.{{ field }} = SRC.{{ field }}
    {% endif %}
  {% endfor %}"""

# Set clause for merge query
set_clause_jinja = """
  {% for field in fields_update %}
    {% if loop.first %}
      {{ field }}=SRC.{{ field }}
    {% else %}
    ,{{ field }} = SRC.{{ field }}
    {% endif %}
  {% endfor %}"""

# Update query
query_update_template = """ UPDATE {{schema_name | sqlsafe}}.{{table_name | sqlsafe}}
SET {{fields_update | sqlsafe}} = :update_value
{% if table_name != 'google_ads_cost_by_device' %}
, last_updated_date=CURRENT_TIMESTAMP
{% endif %}
"""

# Truncate query
query_truncate = """
TRUNCATE TABLE {{schema_name | sqlsafe}}.{{table_name | sqlsafe}}
"""

# Delete query
query_delete = """
DELETE FROM {{schema_name | sqlsafe}}.{{table_name | sqlsafe}}
WHERE {{delete_col | sqlsafe}}>= {{ min_date | sqlsafe}}"""

# Merge query
query_update_template_merge = """
MERGE INTO {{schema_name | sqlsafe}}.{{table_name | sqlsafe}} TGT
USING {{schema_name | sqlsafe}}.{{table_name_source | sqlsafe}} SRC
{{on_clause | sqlsafe}}
WHEN MATCHED THEN
UPDATE SET {{set_clause | sqlsafe}}
{% if table_name != 'google_ads_cost_by_device' %}
,last_updated_date=CURRENT_TIMESTAMP
{% endif %}
"""

# Exclude query
query_exclude_template = """
SELECT DISTINCT {{column_name | sqlsafe}}
FROM {{schema_name | sqlsafe}}.{{table_name | sqlsafe}}
{% if min_date %}
WHERE date>= {{min_date}}
AND date<= {{max_date }}
{% elif min_data %}
WHERE data>= {{min_data}}
AND data<= {{max_data }}
{% elif row_loaded_date %}
WHERE row_loaded_date>=current_date()-200
{% else %}
{% endif %}
"""

# Check update query
query_check_update = """
SELECT DISTINCT {{column_name | sqlsafe}}
FROM {{schema_name | sqlsafe}}.{{table_name | sqlsafe}}
"""

# LinkedIn token query
query_token = """
SELECT ACCESS_TOKEN, REFRESH_TOKEN, EXPIRES
FROM ESPDM.SOCIAL_ADS_POSTS_ACCESS_CODE
WHERE SOCIAL='LinkedinADS' AND
ROW_LOADED_DATE IN ( SELECT MAX(G.ROW_LOADED_DATE) from ESPDM.SOCIAL_ADS_POSTS_ACCESS_CODE G)
"""

# LinkedIn URNs query
query_urns_linkedin = """
SELECT id FROM GoogleAnalytics.linkedin_ads_campaign WHERE ROW_LOADED_DATE>=CURRENT_DATE()-150"""

# LinkedIn creatives query
query_creatives_linkedin = """ SELECT distinct creative_id as id FROM GoogleAnalytics.linkedin_ads_insights
where ROW_LOADED_DATE > CURRENT_DATE() - 180"""

# Create source table query
query_create_table_source = """
CREATE TABLE IF NOT EXISTS {{schema_name | sqlsafe}}.{{ table_name_source | sqlsafe }}
LIKE {{schema_name | sqlsafe}}.{{table_name | sqlsafe}}
"""

# Add last_updated_date column
query_add_last_upd_date = """
ALTER TABLE {{schema_name | sqlsafe}}.{{ table_name | sqlsafe }}
ADD COLUMN IF NOT EXISTS last_updated_date TIMESTAMP"""
