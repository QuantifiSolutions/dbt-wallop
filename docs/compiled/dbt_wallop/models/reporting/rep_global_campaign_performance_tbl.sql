

WITH
final AS (
    SELECT
        cli.client_name,
        cli.client_tag,
        cmp.*
    FROM `dbt-wallop-dev-1`.`reporting`.`mod_global_campaign_performance` cmp
    LEFT JOIN `dbt-wallop-dev-1`.`reporting`.`mod_composite_client_key` cli
        ON CONCAT(
                cmp.account_id,
                IFNULL(cmp.campaign_id, ''),
                IFNULL(cmp.campaign_name, '')
                ) = cli.client_key
        ),

final_w_exclusions AS (
    SELECT *
    FROM final
    WHERE
        NOT (campaign_name IS NOT NULL AND client_name != 'Marina Grand Resort' AND REGEXP_CONTAINS(campaign_name, '(?i)(broad match)'))
        AND NOT (client_name = 'Zingerman' AND campaign_name = 'Bid Strategy')
        AND NOT (client_name = 'The Ranch At Rock Creek' AND campaign_name = 'TRRC - Search - US & CA No extensions')
        )

SELECT * FROM final_w_exclusions