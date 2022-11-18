WITH
gads_core AS (
    SELECT
        'Google Ads' AS data_source,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        adgroup_id,
        adgroup_name,
        CAST(NULL AS STRING) AS ad_id,
        CAST(NULL AS STRING) AS ad_name,
        'google' AS utm_source,
        'cpc' AS utm_medium,
        campaign_name AS utm_campaign,
        headline,
        description,
        cost,
        impressions,
        clicks,
        date,
        row_uuid,
    FROM {{ref('stg_gads__ad_performance')}}
    ),

fb_core AS (
    SELECT
        'Facebook' AS data_source,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        adgroup_id,
        adgroup_name,
        ad_id,
        ad_name,
        utm_source,
        utm_medium,
        utm_campaign,
        headline,
        description,
        cost,
        impressions,
        clicks,
        date,
        row_uuid,
    FROM {{ref('stg_fb__ad_performance')}}
    ),

bing_core AS (
    SELECT
        'Bing' AS data_source,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        CAST(NULL AS STRING) AS adgroup_id,
        CAST(NULL AS STRING) AS adgroup_name,
        CAST(NULL AS STRING) AS ad_id,
        CAST(NULL AS STRING) AS ad_name,
        utm_source,
        utm_medium,
        utm_campaign,
        CAST(NULL AS STRING) AS headline,
        description,
        cost,
        impressions,
        clicks,
        date,
        row_uuid,
    FROM {{ref('stg_bing__ad_performance')}}
    ),

unioned AS (
    SELECT * FROM gads_core

    UNION ALL

    SELECT * FROM fb_core

    UNION ALL

    SELECT * FROM bing_core
    ),

unioned_plus_fields AS (
    SELECT
        u.*,
        STRUCT(gads.ad_type) AS gads,
        STRUCT(
            fb.image_url,
            fb.actions_comment,
            fb.actions_link_click,
            fb.actions_onsite_conversion__post_save,
            fb.actions_post,
            fb.actions_post_engagement,
            fb.actions_post_reaction,
            fb.actions_purchase,
            fb.actions_video_view_video_play
            ) AS fb,
    FROM unioned u
    LEFT JOIN {{ref('stg_gads__ad_performance')}} gads
        ON u.row_uuid = gads.row_uuid
    LEFT JOIN {{ref('stg_fb__ad_performance')}} fb
        ON u.row_uuid = fb.row_uuid
        ),

final AS (
    SELECT
        * EXCEPT (row_uuid, date),
        date,
    FROM unioned_plus_fields
    )

SELECT * FROM final
