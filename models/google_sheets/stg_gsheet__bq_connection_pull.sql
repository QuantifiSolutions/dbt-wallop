SELECT DISTINCT
    a.data_source,
    a.account_id,
    a.account_name,
    SHA256(CONCAT(a.data_source, a.account_id, a.account_name)) AS data_pk,
    b.client_name,
FROM {{ref('mod_global_campaign_performance')}} a
LEFT JOIN {{source('google_sheets', 'account_client_mapping')}} b
    ON SHA256(CONCAT(a.data_source, a.account_id, a.account_name)) = b.data_pk
ORDER BY
    b.client_name