WITH
processing_layer_1 AS (
    SELECT
        CONCAT(cmp.account_id, IFNULL(cmp.campaign_id, '')) AS client_key,
        COALESCE(grp.client_name, gs.client_name) AS client_name
    FROM {{ref('mod_global_campaign_performance')}} cmp
    LEFT JOIN {{source('google_sheets', 'account_client_mapping')}} gs
        ON SHA256(CONCAT(cmp.data_source, cmp.account_id, cmp.account_name)) = gs.data_pk
    LEFT JOIN {{ref('int_group_client_mapping')}} grp
        ON cmp.account_id = grp.account_id
        AND cmp.campaign_id = grp.campaign_id
    GROUP BY
        1, 2
        ),

final AS (
    SELECT
        p1.client_key,
        p1.client_name,
        tag.client_tag
    FROM processing_layer_1 p1
    LEFT JOIN {{source('google_sheets', 'client_tag_mapping')}} tag
        ON p1.client_name = tag.client_name
        )

SELECT * FROM final
