WITH LatestDealStage AS (
    SELECT 
        ds.deal_id,
        ds.stage_id,
        ROW_NUMBER() OVER (PARTITION BY ds.deal_id ORDER BY ds.date_created DESC) AS rn
    FROM deal_stage ds
),
StageCounts AS (
    SELECT 
        o.org_id,
        o.organisation_name,
        o.business AS business,
        c.country_id,
        c.name AS country_name,
        s.display_name AS original_stage_name,
        CASE
            WHEN o.business = 'REFRESH' AND s.display_name = 'Initial Consultation'
            THEN 'Lead In to Initial Consultation'
            WHEN o.business = 'REFRESH' AND s.display_name = 'Concept & Feasibility'
            THEN 'Initial Consultation to Scoping & Feasibility'
            WHEN o.business = 'REFRESH' AND s.display_name = 'Working Drawings & Costing'
            THEN 'Scoping & Feasibility to Detailed Planning & Costing'
            WHEN o.business = 'REFRESH' AND s.display_name = 'Build Stage'
            THEN 'Detailed Planning & Costing to Build Stage'
            WHEN o.business = 'REFRESH' AND s.display_name = 'Project Closure'
            THEN 'Build Stage to Project Completion'
            ELSE s.display_name
        END AS stage_name,
        COUNT(DISTINCT d.deal_id) AS lead_count
    FROM organisation o
    JOIN country c ON c.country_id = o.country_id
    JOIN deal d ON d.org_id = o.org_id
    JOIN LatestDealStage lds ON lds.deal_id = d.deal_id AND lds.rn = 1
    JOIN stage s ON s.stage_id = lds.stage_id
    WHERE d.status IN ('A', 'C') 
      AND d.date_created >= CURRENT_DATE - INTERVAL '365 days' 
      AND o.status = 'A' 
      AND o.business = 'REFRESH'
    GROUP BY o.org_id, o.organisation_name, o.business, c.country_id, c.name, s.display_name
),
ConversionRate AS (
    SELECT 
        org_id,
        organisation_name,
        country_id,
        country_name,
        original_stage_name,
        stage_name,
        lead_count,
        CASE 
            WHEN SUM(lead_count) OVER (PARTITION BY org_id, country_id) = 0 THEN 0
            ELSE lead_count::decimal / SUM(lead_count) OVER (PARTITION BY org_id, country_id)
        END AS conversion_rate
    FROM StageCounts
),
PercentileRanks AS (
    SELECT 
        org_id,
        organisation_name,
        country_id,
        country_name,
        original_stage_name,
        stage_name,
        lead_count,
        conversion_rate,
        PERCENT_RANK() OVER (PARTITION BY stage_name ORDER BY conversion_rate) AS pr_global,
        PERCENT_RANK() OVER (PARTITION BY country_id, stage_name ORDER BY conversion_rate) AS pr_country,
        -- Add percentile ranks for lead counts as well
        PERCENT_RANK() OVER (PARTITION BY stage_name ORDER BY lead_count) AS pr_lead_global,
        PERCENT_RANK() OVER (PARTITION BY country_id, stage_name ORDER BY lead_count) AS pr_lead_country
    FROM ConversionRate
),
QuartileGlobal AS (
    SELECT 
        stage_name,
        'Global' AS category,
        AVG(conversion_rate) FILTER (WHERE pr_global >= 0.75) AS top_quartile_conversion,
        AVG(conversion_rate) FILTER (WHERE pr_global <= 0.25) AS bottom_quartile_conversion,
        -- Calculate average lead counts for top and bottom quartiles
        AVG(lead_count) FILTER (WHERE pr_lead_global >= 0.75) AS top_quartile_lead_count,
        AVG(lead_count) FILTER (WHERE pr_lead_global <= 0.25) AS bottom_quartile_lead_count
    FROM PercentileRanks
    GROUP BY stage_name
),
QuartileCountry AS (
    SELECT 
        country_id,
        country_name AS category,
        stage_name,
        AVG(conversion_rate) FILTER (WHERE pr_country >= 0.75) AS top_quartile_conversion,
        AVG(conversion_rate) FILTER (WHERE pr_country <= 0.25) AS bottom_quartile_conversion,
        -- Calculate average lead counts for top and bottom quartiles by country
        AVG(lead_count) FILTER (WHERE pr_lead_country >= 0.75) AS top_quartile_lead_count,
        AVG(lead_count) FILTER (WHERE pr_lead_country <= 0.25) AS bottom_quartile_lead_count
    FROM PercentileRanks
    GROUP BY country_id, country_name, stage_name
),
AllOrganisationsWithAllBenchmarks AS (
    -- Global benchmarks for all organisations
    SELECT 
        p.org_id,
        p.country_name,
        p.organisation_name,
        p.original_stage_name,
        p.stage_name,
        p.lead_count,
        'Global' AS category,
        p.conversion_rate,
        q.top_quartile_conversion,
        q.bottom_quartile_conversion,
        q.top_quartile_lead_count,  -- Added top quartile lead count
        q.bottom_quartile_lead_count  -- Added bottom quartile lead count
    FROM PercentileRanks p
    CROSS JOIN QuartileGlobal q 
    WHERE q.stage_name = p.stage_name
    
    UNION ALL
    
    -- Country benchmarks for all organisations
    SELECT 
        p.org_id,
        p.country_name,
        p.organisation_name,
        p.original_stage_name,
        p.stage_name,
        p.lead_count,
        q.category,
        p.conversion_rate,
        q.top_quartile_conversion,
        q.bottom_quartile_conversion,
        q.top_quartile_lead_count,  -- Added top quartile lead count
        q.bottom_quartile_lead_count  -- Added bottom quartile lead count
    FROM PercentileRanks p
    CROSS JOIN QuartileCountry q 
    WHERE q.stage_name = p.stage_name
)
SELECT 
    org_id,
    country_name,
    organisation_name,
    original_stage_name,
    stage_name,
    lead_count,
    category,
    conversion_rate,
    top_quartile_conversion,
    bottom_quartile_conversion,
    top_quartile_lead_count,  -- Final output
    bottom_quartile_lead_count,  -- Final output
    -- Additional calculated columns for better insights
    CASE 
        WHEN lead_count > top_quartile_lead_count THEN 'Above Top Quartile'
        WHEN lead_count = top_quartile_lead_count THEN 'At Top Quartile'
        WHEN lead_count < bottom_quartile_lead_count THEN 'Below Bottom Quartile'
        WHEN lead_count = bottom_quartile_lead_count THEN 'At Bottom Quartile'
        ELSE 'Middle 50%'
    END AS lead_count_quartile_position
FROM AllOrganisationsWithAllBenchmarks
ORDER BY organisation_name, stage_name, category;
