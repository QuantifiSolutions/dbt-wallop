SELECT DISTINCT
    a.data_source,
    a.account_id,
    a.account_name,
    SHA256(CONCAT(a.data_source, a.account_id, a.account_name)) AS data_pk,
    b.client_name,
    b.client_tag
FROM {{ref('mod_global_campaign_performance')}} a
LEFT JOIN `bigquery-312020.google_sheets.dbt_wallop_client_mapping_2` b
    ON SHA256(CONCAT(a.data_source, a.account_id, a.account_name)) = b.data_pk
ORDER BY
    b.client_name