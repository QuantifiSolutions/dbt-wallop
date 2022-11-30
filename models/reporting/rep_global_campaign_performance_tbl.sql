{{ config(
    materialized = 'table'
)}}

WITH
processing_layer_1 AS (
    SELECT
        cmp.*,
        COALESCE(grp.client_name, gs.client_name) AS client_name
    FROM {{ref('mod_global_campaign_performance')}} cmp
    LEFT JOIN {{source('google_sheets', 'account_client_mapping')}} gs
        ON SHA256(CONCAT(cmp.data_source, cmp.account_id, cmp.account_name)) = gs.data_pk
    LEFT JOIN {{ref('int_group_client_mapping')}} grp
        ON cmp.account_id = grp.account_id
        AND cmp.campaign_id = grp.campaign_id
        ),

final AS (
    SELECT
        p.client_name,
        tag.client_tag,
        p.* EXCEPT (client_name)
    FROM processing_layer_1 p
    LEFT JOIN {{source('google_sheets', 'client_tag_mapping')}} tag
        ON p.client_name = tag.client_name
        )

SELECT * FROM final
