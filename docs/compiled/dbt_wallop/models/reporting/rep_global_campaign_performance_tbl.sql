

WITH
processing_layer_1 AS (
    SELECT
        cmp.*,
        COALESCE(gs.client_name, grp.client_name) AS client_name
    FROM `dbt-wallop-dev-1`.`reporting`.`mod_global_campaign_performance` cmp
    LEFT JOIN `bigquery-312020`.`google_sheets`.`account_client_mapping` gs
        ON SHA256(CONCAT(cmp.data_source, cmp.account_id, cmp.account_name)) = gs.data_pk
    LEFT JOIN `dbt-wallop-dev-1`.`reporting`.`int_group_client_mapping` grp
        ON cmp.account_id = grp.account_id
        AND cmp.campaign_id = grp.account_id
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