WITH
processing_layer_1 AS (
    SELECT
        account_id,
        account_name,
        NULLIF(campaign_id, '') AS campaign_id,
        NULLIF(campaign_name, '') AS campaign_name,
        NULLIF(adset_id, '') AS adgroup_id,
        NULLIF(adset_name, '') AS adgroup_name,
        NULLIF(ad_id, '') AS ad_id,
        NULLIF(ad_name, '') AS ad_name,
        NULLIF(utm_campaign, '') AS utm_campaign,
        NULLIF(utm_medium, '') AS utm_medium,
        NULLIF(utm_source, '') AS utm_source,
        NULLIF(body, '') AS description,
        NULLIF(title_asset_text, '') AS headline,
        NULLIF(image_url, '') AS image_url,
        spend AS cost,
        reach,
        impressions,
        clicks,
        actions_comment,
        actions_link_click,
        actions_onsite_conversion__post_save,
        actions_post,
        actions_post_engagement,
        actions_post_reaction,
        actions_purchase,
        video_play_actions_video_view AS actions_video_view_video_play,
        DATE(date) AS `date`,
    FROM {{source('analytics_wallop', 'pma_facebook_ads_performance_ad_level')}}
    ),

final AS (
    SELECT
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        adgroup_id,
        adgroup_name,
        ad_id,
        ad_name,
        utm_campaign,
        utm_medium,
        utm_source,
        description,
        headline,
        image_url,
        cost,
        reach,
        impressions,
        clicks,
        actions_comment,
        actions_link_click,
        actions_onsite_conversion__post_save,
        actions_post,
        actions_post_engagement,
        actions_post_reaction,
        actions_purchase,
        actions_video_view_video_play,
        `date`,
    FROM processing_layer_1
    WHERE
        campaign_id IS NOT NULL
        )

SELECT * FROM final

