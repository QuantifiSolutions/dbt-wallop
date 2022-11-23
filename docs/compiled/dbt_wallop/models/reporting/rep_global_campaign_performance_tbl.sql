
WITH
final AS (
SELECT
    gs.client_name,
    gs.client_tag,
    cmp.*
FROM `dbt-wallop-dev-1`.`reporting`.`mod_global_campaign_performance` cmp
LEFT JOIN `dbt-wallop-dev-1`.`google_sheets`.`stg_gsheet__bq_connection_pull` gs
    ON cmp.data_source = gs.data_source
    AND cmp.account_id = gs.account_id
    )

SELECT * FROM final