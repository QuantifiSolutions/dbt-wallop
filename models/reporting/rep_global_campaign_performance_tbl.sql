{{ config(
    materialized = 'table'
)}}

WITH
final AS (
    SELECT
        cli.client_name,
        cli.client_tag,
        cmp.*
    FROM {{ref('mod_global_campaign_performance')}} cmp
    LEFT JOIN {{ref('mod_composite_client_key')}} cli
        ON CONCAT(cmp.account_id, IFNULL(cmp.campaign_id, '')) = cli.client_key
        )

SELECT * FROM final
