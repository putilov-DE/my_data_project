-- Campaign Performance.
SELECT c.campaign_name, top_c.CTR, top_c.total_cost from
(SELECT e.campaign_id ,
SUM(e.ad_cost) as total_cost,
ROUND(
(SUM(case when ad_revenue > 0 then 1 else 0 END )-- field was_clicked is always 0 that's why it's counted like that
/
NULLIF(COUNT(e.event_id), 0) *100)	
,2) as CTR
from events e 
WHERE e.event_timestamp >= '2024-11-01' AND e.event_timestamp < '2024-12-01'
group by campaign_id 
order by CTR DESC
limit 5) as top_c
left join campaigns c on top_c.campaign_id = c.campaign_id
order by top_c.CTR DESC
-- ------------------------------------------------------------------------------------------------------- 
-- Advertiser Spending
WITH Campaign_Agg AS (
    -- STEP 1: Compress millions of raw events into campaigns FIRST (No JOINs yet, lightning fast)
    SELECT 
        campaign_id,
        SUM(ad_cost) AS camp_cost,
        SUM(CASE WHEN ad_revenue > 0 THEN 1 ELSE 0 END) AS camp_clicks,
        COUNT(event_id) AS camp_events
    FROM events 
    WHERE event_timestamp >= '2024-11-01' AND event_timestamp < '2024-12-01'
    GROUP BY campaign_id
),
Top_Advertisers AS (
    -- STEP 2: Join the compressed data with campaigns, group by advertiser, and take Top 5
    SELECT 
        c.advertiser_id,
        SUM(ca.camp_cost) AS total_cost,
        SUM(ca.camp_clicks) AS total_clicks,
        SUM(ca.camp_events) AS total_events
    FROM Campaign_Agg ca
    JOIN campaigns c ON ca.campaign_id = c.campaign_id
    GROUP BY c.advertiser_id
    ORDER BY total_cost DESC
    LIMIT 5
)
-- STEP 3: Join the final top 5 to get names and calculate the heavy CTR formula
SELECT 
    a.advertiser_name,
    ta.total_cost,
    
    -- Calculate CTR only for the Top 5 advertisers
    ROUND(
        (ta.total_clicks / NULLIF(ta.total_events, 0)) * 100
    , 2) AS CTR

FROM Top_Advertisers ta
JOIN advertisers a ON ta.advertiser_id = a.advertiser_id
ORDER BY ta.total_cost DESC;

-- -----------------------------------------------------------------------------
-- Cost Efficiency
WITH Campaign_Agg AS (
    -- STEP 1: Compress millions of raw events (Lightning fast)
    SELECT 
        location_id,
        campaign_id,
        SUM(ad_cost) AS camp_cost,
        SUM(ad_revenue) AS camp_revenue, -- Added revenue to answer the business question
        SUM(CASE WHEN ad_revenue > 0 THEN 1 ELSE 0 END) AS camp_clicks,
        COUNT(event_id) AS camp_events
    FROM events 
    WHERE event_timestamp >= '2024-11-01' AND event_timestamp < '2024-12-01'
    GROUP BY location_id, campaign_id
),
Top_Countries AS (
    -- STEP 2: Join locations and take the Top 30 by Revenue
    SELECT 
        l.country_name, 
        ca.campaign_id,
        SUM(ca.camp_cost) AS total_cost,
        SUM(ca.camp_revenue) AS total_revenue, 
        SUM(ca.camp_clicks) AS total_clicks,
        SUM(ca.camp_events) AS total_events
    FROM Campaign_Agg ca
    JOIN locations l ON l.location_id = ca.location_id 
    GROUP BY l.country_name, ca.campaign_id
    ORDER BY total_revenue DESC 
    LIMIT 30 
)
-- STEP 3: Final math calculation
SELECT 
    tc.country_name, 
    tc.campaign_id,
    tc.total_revenue, 
    tc.total_cost,
    
    -- CPM: Cost Per Mille (Multiplied by 1000)
    ROUND(
        (tc.total_cost / NULLIF(tc.total_events, 0)) * 1000
    , 2) AS CPM,
    
    -- CPC: Cost Per Click 
    ROUND(
        (tc.total_cost / NULLIF(tc.total_clicks, 0))
    , 2) AS CPC

FROM Top_Countries tc
ORDER BY tc.total_revenue DESC; 

-- --------------------------------------------------------------------------------------------------
-- Regional Analysis
WITH Location_Revenue AS (
 
    -- include all events to accurately calculate total costs.
    SELECT 
        location_id,
        SUM(ad_revenue) AS total_revenue,
        SUM(ad_cost) AS total_cost,
        SUM(CASE WHEN ad_revenue > 0 THEN 1 ELSE 0 END) AS total_clicks
    FROM events 
    WHERE event_timestamp >= '2024-11-01' AND event_timestamp < '2024-12-01'
    GROUP BY location_id
    
    -- keep the filter here (in HAVING) to exclude countries
    -- that generated zero revenue, but only after all the math is done.
    HAVING SUM(ad_revenue) > 0 
    ORDER BY total_revenue DESC
    LIMIT 10
)
SELECT 
    l.country_name,
    lr.total_revenue,
    lr.total_clicks,
    
    -- ROAS: Return on Ad Spend (Revenue / Cost)
    -- use NULLIF to prevent division by zero in case cost is 0
    ROUND(
        (lr.total_revenue / NULLIF(lr.total_cost, 0))
    , 2) AS ROAS

FROM Location_Revenue lr
JOIN locations l ON lr.location_id = l.location_id
ORDER BY lr.total_revenue DESC;


-- additional for the most engaged users
SELECT 
    user_id,
    COUNT(event_id) AS total_impressions,
    SUM(CASE WHEN ad_revenue > 0 THEN 1 ELSE 0 END) AS total_clicks,
    
    -- Calculate Personal CTR for the user
    ROUND(
        (SUM(CASE WHEN ad_revenue > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(event_id), 0)) * 100
    , 2) AS user_CTR
    
FROM events
WHERE event_timestamp >= '2024-11-01' AND event_timestamp < '2024-12-01'
GROUP BY user_id
-- Filter out users with zero engagement using the alias
HAVING total_clicks > 0 
ORDER BY total_clicks DESC, user_CTR DESC
LIMIT 20;
-- ----------------------------------------------------------------------------------------------

WITH Campaign_Spend AS (
    -- STEP 1: Calculate actual money spent by each campaign
    SELECT 
        campaign_id,
        SUM(ad_cost) AS total_spent
    FROM events 
    WHERE event_timestamp >= '2024-10-01' AND event_timestamp < '2025-12-01'
    GROUP BY campaign_id
)
-- STEP 2: Compare spent money with the planned budget
SELECT 
    c.campaign_name,
    c.budget,
    cs.total_spent,
    
    -- REMAINING BUDGET: Select the existing column directly from the campaigns table
    c.remaining_budget, 
    
    -- Calculate the percentage of budget consumed
    ROUND((cs.total_spent / NULLIF(c.budget, 0)) * 100, 2) AS budget_used_pct
    
FROM Campaign_Spend cs
JOIN campaigns c ON cs.campaign_id = c.campaign_id

-- FILTER: Only show campaigns that consumed more than 80% (> 0.8) of their budget
WHERE (cs.total_spent / NULLIF(c.budget, 0)) > 0.8

ORDER BY budget_used_pct DESC;

-- Do certain types of ads perform better on mobile than desktop? 
SELECT 
    c.campaign_name, 
    e.device,
    COUNT(e.event_id) AS total_impressions,
    SUM(CASE WHEN e.ad_revenue > 0 THEN 1 ELSE 0 END) AS total_clicks,
    SUM(e.ad_revenue) AS total_revenue,
    
    -- CTR: Click-Through Rate per device for this campaign
    ROUND(
        (SUM(CASE WHEN e.ad_revenue > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(e.event_id), 0)) * 100
    , 2) AS CTR,
    
    -- ROAS: Return on Ad Spend per device for this campaign
    ROUND(
        (SUM(e.ad_revenue) / NULLIF(SUM(e.ad_cost), 0))
    , 2) AS ROAS

FROM events e
JOIN campaigns c ON e.campaign_id = c.campaign_id
WHERE e.event_timestamp >= '2024-10-01' AND e.event_timestamp < '2024-12-01'
GROUP BY 
    c.campaign_name, 
    e.device
ORDER BY 
    c.campaign_name, 
    CTR DESC;
-- --------------------------------------------------------------------------------------------------

-- Compare CTR across different device types
SELECT 
    device,
    COUNT(event_id) AS total_impressions,
    SUM(CASE WHEN ad_revenue > 0 THEN 1 ELSE 0 END) AS total_clicks,
    
    -- Calculate CTR: (Clicks / Impressions) * 100
    ROUND(
        (SUM(CASE WHEN ad_revenue > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(event_id), 0)) * 100
    , 2) AS CTR
    
FROM events
WHERE event_timestamp >= '2024-10-01' AND event_timestamp < '2024-12-01'
GROUP BY device
ORDER BY CTR DESC;