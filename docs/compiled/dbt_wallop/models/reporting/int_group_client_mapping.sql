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
            WHEN account_id = '50194384' AND REGEXP_CONTAINS(campaign_name, '(?i)(southall)') THEN 'Southall Farm'
            WHEN account_id = '50194384' AND REGEXP_CONTAINS(campaign_name, '(?i)(lord elgin)') THEN 'Lord Elgin'
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
            WHEN account_id = '4178754205' AND REGEXP_CONTAINS(campaign_name, '(?i)(modavie)') THEN 'Modavie'
            -- Facebook - 467804157855475 - Mirival
            WHEN account_id = '467804157855475' AND REGEXP_CONTAINS(campaign_name, '(?i)(austin)') THEN 'Miraval Resorts Austin'
            WHEN account_id = '467804157855475' AND REGEXP_CONTAINS(campaign_name, '(?i)(arizona)') THEN 'Miraval Resorts Arizona'
            WHEN account_id = '467804157855475' AND REGEXP_CONTAINS(campaign_name, '(?i)(new england|berkshire)') THEN 'Miraval Resorts Berkshire'
            -- Facebook - 759778052430 - Scandinave Spa
            WHEN account_id = '759778052430' AND REGEXP_CONTAINS(campaign_name, '(?i)(blue mountain)') THEN 'Scandinave Spa Blue Mountain'
            WHEN account_id = '759778052430' AND REGEXP_CONTAINS(campaign_name, '(?i)(tremblant)') THEN 'Scandinave Spa Mont-Tremblant'
            WHEN account_id = '759778052430' AND REGEXP_CONTAINS(campaign_name, '(?i)(montreal)') THEN 'Scandinave Spa Montreal'
            WHEN account_id = '759778052430' AND REGEXP_CONTAINS(campaign_name, '(?i)(whistler)') THEN 'Scandinave Spa Whistler'
        END AS client_name
    FROM `dbt-wallop-dev-1`.`reporting`.`mod_global_campaign_performance`
    WHERE
        account_id IN (
            '50194384',
            '4178754205',
            '467804157855475',
            '759778052430'
            )
            )

SELECT * FROM client_mapping