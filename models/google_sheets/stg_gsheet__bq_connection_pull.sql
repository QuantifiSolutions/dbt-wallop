SELECT DISTINCT
    a.data_source,
    a.account_id,
    a.account_name,
    b.client_name,
    b.client_tag
FROM {{ref('mod_global_campaign_performance')}} a
LEFT JOIN `bigquery-312020.google_sheets.dbt_wallop_client_mapping` b
    ON a.data_source = b.data_source
    AND CAST(a.account_id AS STRING) = CAST(b.account_id AS STRING)
    AND a.account_name = b.account_name
ORDER BY
    b.client_name