SELECT DISTINCT
    a.data_source,
    a.account_id,
    a.account_name,
    SHA256(CONCAT(a.data_source, a.account_id, a.account_name)) AS data_pk,
    b.client_name,
FROM `dbt-wallop-dev-1`.`reporting`.`mod_global_campaign_performance` a
LEFT JOIN `bigquery-312020`.`google_sheets`.`account_client_mapping` b
    ON SHA256(CONCAT(a.data_source, a.account_id, a.account_name)) = b.data_pk
ORDER BY
    b.client_name