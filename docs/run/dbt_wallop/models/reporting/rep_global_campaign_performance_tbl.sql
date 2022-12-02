
  
    

    create or replace table `dbt-wallop-dev-1`.`reporting`.`rep_global_campaign_performance_tbl`
    
    
    OPTIONS()
    as (
      

WITH
final AS (
    SELECT
        cli.client_name,
        cli.client_tag,
        cmp.*
    FROM `dbt-wallop-dev-1`.`reporting`.`mod_global_campaign_performance` cmp
    LEFT JOIN `dbt-wallop-dev-1`.`reporting`.`mod_composite_client_key` cli
        ON CONCAT(cmp.account_id, IFNULL(cmp.campaign_id, '')) = cli.client_key
        )

SELECT * FROM final
    );
  