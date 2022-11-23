
WITH
final AS (
SELECT
    gs.client_name,
    gs.client_tag,
    ad.*
FROM `dbt-wallop-dev-1`.`reporting`.`mod_global_ad_performance` ad
LEFT JOIN `dbt-wallop-dev-1`.`google_sheets`.`stg_gsheet__bq_connection_pull` gs
    ON ad.data_source = gs.data_source
    AND ad.account_id = gs.account_id
    )

SELECT * FROM final