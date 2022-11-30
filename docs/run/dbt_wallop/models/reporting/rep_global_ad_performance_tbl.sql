
  
    

    create or replace table `dbt-wallop-dev-1`.`reporting`.`rep_global_ad_performance_tbl`
    
    
    OPTIONS()
    as (
      

WITH
processing_layer_1 AS (
    SELECT
        ad.*,
        COALESCE(grp.client_name, gs.client_name) AS client_name
    FROM `dbt-wallop-dev-1`.`reporting`.`mod_global_ad_performance` ad
    LEFT JOIN `bigquery-312020`.`google_sheets`.`account_client_mapping` gs
        ON SHA256(CONCAT(ad.data_source, ad.account_id, ad.account_name)) = gs.data_pk
    LEFT JOIN `dbt-wallop-dev-1`.`reporting`.`int_group_client_mapping` grp
        ON ad.account_id = grp.account_id
        AND ad.campaign_id = grp.campaign_id
        ),

final AS (
    SELECT
        p.client_name,
        tag.client_tag,
        p.* EXCEPT (client_name)
    FROM processing_layer_1 p
    LEFT JOIN `bigquery-312020`.`google_sheets`.`client_tag_mapping` tag
        ON p.client_name = tag.client_name
        )

SELECT * FROM final
    );
  