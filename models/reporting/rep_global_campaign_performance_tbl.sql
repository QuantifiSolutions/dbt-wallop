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
    ON SHA256(CONCAT(cmp.data_source, cmp.account_id, cmp.account_name)) = gs.data_pk
    )

SELECT * FROM final
