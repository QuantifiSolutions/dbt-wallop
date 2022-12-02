

WITH
final AS (
    SELECT
        cli.client_name,
        cli.client_tag,
        ad.*
    FROM `dbt-wallop-dev-1`.`reporting`.`mod_global_ad_performance` ad
    LEFT JOIN `dbt-wallop-dev-1`.`reporting`.`mod_composite_client_key` cli
        ON CONCAT(ad.account_id, IFNULL(ad.campaign_id, '')) = cli.client_key
        )

SELECT * FROM final