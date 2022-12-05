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
        )

SELECT * FROM final
WHERE
    NOT REGEXP_CONTAINS(campaign_name, '(?i)(broad match)')