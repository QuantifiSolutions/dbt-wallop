{{ config(
    materialized = 'table'
)}}
WITH
final AS (
SELECT
    gs.client_name,
    gs.client_tag,
    cmp.*
FROM {{ref('mod_global_campaign_performance')}} cmp
LEFT JOIN {{ref('stg_gsheet__bq_connection_pull')}} gs
    ON cmp.data_source = gs.data_source
    AND cmp.account_id = gs.account_id
    )

SELECT * FROM final
