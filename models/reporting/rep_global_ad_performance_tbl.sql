{{ config(
    materialized = 'table'
)}}
WITH
final AS (
SELECT
    gs.client_name,
    gs.client_tag,
    ad.*
FROM {{ref('mod_global_ad_performance')}} ad
LEFT JOIN {{ref('stg_gsheet__bq_connection_pull')}} gs
    ON ad.data_source = gs.data_source
    AND ad.account_id = gs.account_id
    )

SELECT * FROM final
