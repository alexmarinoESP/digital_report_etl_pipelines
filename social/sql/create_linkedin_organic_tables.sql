-- ============================================================================
-- LinkedIn Organic Posts - Database Tables for Vertica
-- Schema: GoogleAnalytics
-- Created: 2026-01-28
--
-- Eseguire le query in ordine su Vertica
-- ============================================================================


-- ============================================================================
-- 1. LINKEDIN_ORGANIC_PAGES
-- Anagrafica pagine LinkedIn da monitorare
-- ============================================================================
CREATE TABLE IF NOT EXISTS GoogleAnalytics.linkedin_organic_pages (
    organization_id     VARCHAR(20) NOT NULL,
    organization_name   VARCHAR(255),
    vanity_name         VARCHAR(100),
    companyid           INTEGER,
    website_url         VARCHAR(500),
    industry            VARCHAR(100),
    follower_count      BIGINT,
    row_loaded_date     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_date   TIMESTAMP
);

ALTER TABLE GoogleAnalytics.linkedin_organic_pages
ADD CONSTRAINT pk_linkedin_organic_pages PRIMARY KEY (organization_id);


-- ============================================================================
-- 2. LINKEDIN_ORGANIC_POSTS
-- Contenuto e metadati dei post organici
-- ============================================================================
CREATE TABLE IF NOT EXISTS GoogleAnalytics.linkedin_organic_posts (
    post_id             VARCHAR(50) NOT NULL,
    organization_id     VARCHAR(20) NOT NULL,
    commentary          VARCHAR(10000),
    visibility          VARCHAR(30),
    lifecycle_state     VARCHAR(30),
    content_type        VARCHAR(30),
    media_url           VARCHAR(1000),
    media_title         VARCHAR(500),
    post_url            VARCHAR(500),
    is_reshare          BOOLEAN DEFAULT FALSE,
    original_post_id    VARCHAR(50),
    created_at          TIMESTAMP,
    published_at        TIMESTAMP,
    last_modified_at    TIMESTAMP,
    row_loaded_date     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_date   TIMESTAMP
);

ALTER TABLE GoogleAnalytics.linkedin_organic_posts
ADD CONSTRAINT pk_linkedin_organic_posts PRIMARY KEY (post_id);


-- ============================================================================
-- 3. LINKEDIN_ORGANIC_POSTS_INSIGHTS
-- Metriche engagement per ogni post (totali lifetime, aggiornate via UPSERT)
-- ============================================================================
CREATE TABLE IF NOT EXISTS GoogleAnalytics.linkedin_organic_posts_insights (
    post_id                 VARCHAR(50) NOT NULL,
    organization_id         VARCHAR(20) NOT NULL,
    impression_count        BIGINT DEFAULT 0,
    unique_impression_count BIGINT DEFAULT 0,
    click_count             BIGINT DEFAULT 0,
    like_count              BIGINT DEFAULT 0,
    comment_count           BIGINT DEFAULT 0,
    share_count             BIGINT DEFAULT 0,
    engagement              DECIMAL(12,8) DEFAULT 0,
    video_views             BIGINT DEFAULT 0,
    video_view_time_ms      BIGINT DEFAULT 0,
    row_loaded_date         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_date       TIMESTAMP
);

ALTER TABLE GoogleAnalytics.linkedin_organic_posts_insights
ADD CONSTRAINT pk_linkedin_organic_posts_insights PRIMARY KEY (post_id);


-- ============================================================================
-- 4. LINKEDIN_ORGANIC_POSTS_INSIGHTS_SOURCE
-- Tabella source per operazioni MERGE
-- ============================================================================
CREATE TABLE IF NOT EXISTS GoogleAnalytics.linkedin_organic_posts_insights_source (
    post_id                 VARCHAR(50) NOT NULL,
    organization_id         VARCHAR(20) NOT NULL,
    impression_count        BIGINT DEFAULT 0,
    unique_impression_count BIGINT DEFAULT 0,
    click_count             BIGINT DEFAULT 0,
    like_count              BIGINT DEFAULT 0,
    comment_count           BIGINT DEFAULT 0,
    share_count             BIGINT DEFAULT 0,
    engagement              DECIMAL(12,8) DEFAULT 0,
    video_views             BIGINT DEFAULT 0,
    video_view_time_ms      BIGINT DEFAULT 0,
    row_loaded_date         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_date       TIMESTAMP
);


-- ============================================================================
-- 5. LINKEDIN_ORGANIC_PAGE_STATS
-- Statistiche visualizzazioni pagina giornaliere
-- ============================================================================
CREATE TABLE IF NOT EXISTS GoogleAnalytics.linkedin_organic_page_stats (
    organization_id         VARCHAR(20) NOT NULL,
    date                    DATE NOT NULL,
    all_page_views          BIGINT DEFAULT 0,
    unique_page_views       BIGINT DEFAULT 0,
    all_desktop_page_views  BIGINT DEFAULT 0,
    all_mobile_page_views   BIGINT DEFAULT 0,
    overview_page_views     BIGINT DEFAULT 0,
    careers_page_views      BIGINT DEFAULT 0,
    jobs_page_views         BIGINT DEFAULT 0,
    life_at_page_views      BIGINT DEFAULT 0,
    desktop_custom_button_clicks BIGINT DEFAULT 0,
    mobile_custom_button_clicks  BIGINT DEFAULT 0,
    row_loaded_date         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_date       TIMESTAMP
);

ALTER TABLE GoogleAnalytics.linkedin_organic_page_stats
ADD CONSTRAINT pk_linkedin_organic_page_stats PRIMARY KEY (organization_id, date);


-- ============================================================================
-- 6. LINKEDIN_ORGANIC_PAGE_STATS_SOURCE
-- Tabella source per operazioni MERGE
-- ============================================================================
CREATE TABLE IF NOT EXISTS GoogleAnalytics.linkedin_organic_page_stats_source (
    organization_id         VARCHAR(20) NOT NULL,
    date                    DATE NOT NULL,
    all_page_views          BIGINT DEFAULT 0,
    unique_page_views       BIGINT DEFAULT 0,
    all_desktop_page_views  BIGINT DEFAULT 0,
    all_mobile_page_views   BIGINT DEFAULT 0,
    overview_page_views     BIGINT DEFAULT 0,
    careers_page_views      BIGINT DEFAULT 0,
    jobs_page_views         BIGINT DEFAULT 0,
    life_at_page_views      BIGINT DEFAULT 0,
    desktop_custom_button_clicks BIGINT DEFAULT 0,
    mobile_custom_button_clicks  BIGINT DEFAULT 0,
    row_loaded_date         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_date       TIMESTAMP
);


-- ============================================================================
-- 7. LINKEDIN_ORGANIC_FOLLOWERS
-- Statistiche e crescita follower giornaliera
-- ============================================================================
CREATE TABLE IF NOT EXISTS GoogleAnalytics.linkedin_organic_followers (
    organization_id         VARCHAR(20) NOT NULL,
    date                    DATE NOT NULL,
    total_followers         BIGINT,
    organic_follower_gain   INTEGER DEFAULT 0,
    paid_follower_gain      INTEGER DEFAULT 0,
    net_follower_change     INTEGER DEFAULT 0,
    row_loaded_date         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_date       TIMESTAMP
);

ALTER TABLE GoogleAnalytics.linkedin_organic_followers
ADD CONSTRAINT pk_linkedin_organic_followers PRIMARY KEY (organization_id, date);


-- ============================================================================
-- 8. LINKEDIN_ORGANIC_FOLLOWERS_SOURCE
-- Tabella source per operazioni MERGE
-- ============================================================================
CREATE TABLE IF NOT EXISTS GoogleAnalytics.linkedin_organic_followers_source (
    organization_id         VARCHAR(20) NOT NULL,
    date                    DATE NOT NULL,
    total_followers         BIGINT,
    organic_follower_gain   INTEGER DEFAULT 0,
    paid_follower_gain      INTEGER DEFAULT 0,
    net_follower_change     INTEGER DEFAULT 0,
    row_loaded_date         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_date       TIMESTAMP
);


-- ============================================================================
-- 9. LINKEDIN_ORGANIC_FOLLOWER_DEMOGRAPHICS
-- Breakdown follower per demografia (snapshot lifetime)
-- ============================================================================
CREATE TABLE IF NOT EXISTS GoogleAnalytics.linkedin_organic_follower_demographics (
    organization_id         VARCHAR(20) NOT NULL,
    demographic_type        VARCHAR(30) NOT NULL,
    demographic_value       VARCHAR(100) NOT NULL,
    demographic_urn         VARCHAR(100),
    organic_follower_count  BIGINT DEFAULT 0,
    paid_follower_count     BIGINT DEFAULT 0,
    row_loaded_date         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_date       TIMESTAMP
);

ALTER TABLE GoogleAnalytics.linkedin_organic_follower_demographics
ADD CONSTRAINT pk_linkedin_organic_follower_demo PRIMARY KEY (organization_id, demographic_type, demographic_value);


-- ============================================================================
-- INSERT DATI INIZIALI - Organizzazioni da monitorare
-- ============================================================================
INSERT INTO GoogleAnalytics.linkedin_organic_pages
    (organization_id, organization_name, companyid, row_loaded_date)
VALUES
    ('1788340', 'V-Valley the value of Esprinet', 1, CURRENT_TIMESTAMP),
    ('17857', 'Esprinet', 1, CURRENT_TIMESTAMP);


-- ============================================================================
-- VISTE PER REPORTING
-- ============================================================================

-- Vista combinata post + metriche per report
CREATE OR REPLACE VIEW GoogleAnalytics.v_linkedin_organic_posts_report AS
SELECT
    p.post_id,
    p.organization_id,
    pg.organization_name,
    pg.companyid,
    p.commentary,
    p.content_type,
    p.visibility,
    p.lifecycle_state,
    p.created_at,
    p.published_at,
    p.post_url,
    p.media_url,
    p.is_reshare,
    i.impression_count,
    i.unique_impression_count,
    i.click_count,
    i.like_count,
    i.comment_count,
    i.share_count,
    i.engagement,
    i.video_views,
    (i.like_count + i.comment_count + i.share_count) AS total_reactions,
    i.last_updated_date AS metrics_updated_at
FROM GoogleAnalytics.linkedin_organic_posts p
LEFT JOIN GoogleAnalytics.linkedin_organic_posts_insights i ON p.post_id = i.post_id
LEFT JOIN GoogleAnalytics.linkedin_organic_pages pg ON p.organization_id = pg.organization_id;


-- Vista trend follower per report
CREATE OR REPLACE VIEW GoogleAnalytics.v_linkedin_organic_followers_trend AS
SELECT
    f.organization_id,
    pg.organization_name,
    pg.companyid,
    f.date,
    f.total_followers,
    f.organic_follower_gain,
    f.paid_follower_gain,
    f.net_follower_change,
    SUM(f.organic_follower_gain) OVER (
        PARTITION BY f.organization_id
        ORDER BY f.date
        ROWS UNBOUNDED PRECEDING
    ) AS cumulative_organic_gain
FROM GoogleAnalytics.linkedin_organic_followers f
LEFT JOIN GoogleAnalytics.linkedin_organic_pages pg ON f.organization_id = pg.organization_id;


-- Vista statistiche pagina con dettagli organizzazione
CREATE OR REPLACE VIEW GoogleAnalytics.v_linkedin_organic_page_stats_report AS
SELECT
    ps.organization_id,
    pg.organization_name,
    pg.companyid,
    ps.date,
    ps.all_page_views,
    ps.unique_page_views,
    ps.all_desktop_page_views,
    ps.all_mobile_page_views,
    ps.overview_page_views,
    ps.careers_page_views,
    ps.jobs_page_views,
    (ps.desktop_custom_button_clicks + ps.mobile_custom_button_clicks) AS total_button_clicks,
    ps.last_updated_date
FROM GoogleAnalytics.linkedin_organic_page_stats ps
LEFT JOIN GoogleAnalytics.linkedin_organic_pages pg ON ps.organization_id = pg.organization_id;


-- ============================================================================
-- QUERY DI VERIFICA (eseguire dopo creazione per controllare)
-- ============================================================================
-- SELECT table_name FROM v_catalog.tables WHERE table_schema = 'GoogleAnalytics' AND table_name LIKE 'linkedin_organic%';
-- SELECT * FROM GoogleAnalytics.linkedin_organic_pages;
