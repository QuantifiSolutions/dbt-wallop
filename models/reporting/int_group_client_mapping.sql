WITH
client_mapping AS (
    SELECT DISTINCT
        -- fields to join back onto global_(campaign/ad)_performance models
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        -- processing rules for clients
        CASE
            -- Bing - 50194384 - Wallop Creative
            WHEN account_id = '50194384' AND REGEXP_CONTAINS(campaign_name, '(?i)(kea lani)') THEN 'Fairmont Kea Lani'
            WHEN account_id = '50194384' AND REGEXP_CONTAINS(campaign_name, '(?i)(orchid)') THEN 'Fairmont Orchid'
            WHEN account_id = '50194384' AND REGEXP_CONTAINS(campaign_name, '(?i)(san francisco)') THEN 'Fairmont San Francisco'
            WHEN account_id = '50194384' AND REGEXP_CONTAINS(campaign_name, '(?i)(zingerman)') THEN 'Zingerman'
            WHEN account_id = '50194384' AND REGEXP_CONTAINS(campaign_name, '(?i)(harbor grand)') THEN 'Harbor Grand Hotel'
            WHEN account_id = '50194384' AND REGEXP_CONTAINS(campaign_name, '(?i)(marina grand)') THEN 'Marina Grand Resort'
            WHEN account_id = '50194384' AND REGEXP_CONTAINS(campaign_name, '(?i)(trrc)') THEN 'The Ranch At Rock Creek'
            -- Google Ads - 4178754205 - Corner Collection
            WHEN account_id = '4178754205' AND REGEXP_CONTAINS(campaign_name, '(?i)(spa william gray)') THEN 'Spa William Gray'
            WHEN account_id = '4178754205' AND REGEXP_CONTAINS(campaign_name, '(?i)(petit hotel)') THEN 'Petit Hotel'
            WHEN account_id = '4178754205' AND REGEXP_CONTAINS(campaign_name, "(?i)(place d'armes)") THEN "Hotel Place D'Armes"
            WHEN account_id = '4178754205' AND REGEXP_CONTAINS(campaign_name, '(?i)(hotel william gray)') THEN 'Hotel William Gray'
            WHEN account_id = '4178754205' AND REGEXP_CONTAINS(campaign_name, '(?i)(auberge du vieux-port)') THEN 'Auberge du Vieux-Port'
            WHEN account_id = '4178754205' AND REGEXP_CONTAINS(campaign_name, '(?i)(hotel nelligan)') THEN 'Hotel Nelligan'
            WHEN account_id = '4178754205' AND REGEXP_CONTAINS(campaign_name, '(?i)(mechant boeuf)') THEN 'Mechant Boeuf'
            WHEN account_id = '4178754205' AND REGEXP_CONTAINS(campaign_name, '(?i)(nelli bistro)') THEN 'Nelli Bistro'
            WHEN account_id = '4178754205' AND REGEXP_CONTAINS(campaign_name, '(?i)(kyo sushi)') THEN 'Kyo Sushi Bar'
            WHEN account_id = '4178754205' AND REGEXP_CONTAINS(campaign_name, '(?i)(rainspa)') THEN 'Rainspa'
        END AS client_name
    FROM {{ref('mod_global_campaign_performance')}}
    )

SELECT * FROM client_mapping
