{{ config(
    materialized = 'table'
)}}

WITH
final AS (
    SELECT
        cli.client_name,
        cli.client_tag,
        ad.*
    FROM {{ref('mod_global_ad_performance')}} ad
    LEFT JOIN {{ref('mod_composite_client_key')}} cli
        ON CONCAT(
                ad.account_id,
                IFNULL(ad.campaign_id, ''),
                IFNULL(ad.campaign_name, '')
                ) = cli.client_key
        ),

final_w_exclusions AS (
    SELECT *
    FROM final
    WHERE
        NOT (campaign_name IS NOT NULL AND REGEXP_CONTAINS(campaign_name, '(?i)(broad match)'))
        AND NOT (client_name = 'Zingerman' AND campaign_name = 'Bid Strategy')
        AND NOT (client_name = 'The Ranch At Rock Creek' AND campaign_name = 'TRRC - Search - US & CA No extensions')
        )

SELECT * FROM final_w_exclusions
