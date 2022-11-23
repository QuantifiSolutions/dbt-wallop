
WITH
final AS (
SELECT
    gs.client_name,
    gs.client_tag,
    cmp.*
FROM `dbt-wallop-dev-1`.`reporting`.`mod_global_campaign_performance` cmp
LEFT JOIN `dbt-wallop-dev-1`.`google_sheets`.`stg_gsheet__bq_connection_pull` gs
    ON SHA256(CONCAT(cmp.data_source, cmp.account_id, cmp.account_name)) = gs.data_pk
    )

SELECT * FROM final