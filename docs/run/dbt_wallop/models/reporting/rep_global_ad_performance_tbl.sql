
  
    

    create or replace table `dbt-wallop-dev-1`.`reporting`.`rep_global_ad_performance_tbl`
    
    
    OPTIONS()
    as (
      

WITH
final AS (
    SELECT
        cli.client_name,
        cli.client_tag,
        ad.*
    FROM `dbt-wallop-dev-1`.`reporting`.`mod_global_ad_performance` ad
    LEFT JOIN `dbt-wallop-dev-1`.`reporting`.`mod_composite_client_key` cli
        ON CONCAT(
                ad.account_id,
                IFNULL(ad.campaign_id, ''),
                IFNULL(ad.campaign_name, '')
                ) = cli.client_key
        ),

final_w_exclusions AS (
    SELECT *
    FROM final
    WHERE
        NOT (campaign_name IS NOT NULL AND REGEXP_CONTAINS(campaign_name, '(?i)(broad match)'))
        AND NOT (client_name = 'Zingerman' AND campaign_name = 'Bid Strategy')
        AND NOT (client_name = 'The Ranch At Rock Creek' AND campaign_name = 'TRRC - Search - US & CA No extensions')
        )

SELECT * FROM final_w_exclusions
    );
  