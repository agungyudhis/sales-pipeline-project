INSERT INTO website.fct_traffic (
    session_id,
    channel_id,
    device_id,
    visit_date_id,
    visit_time,
    page_views,
    session_duration,
    clicks,
    transactions,
    created_at,
    updated_at
)
WITH new_data AS (
    SELECT DISTINCT
        st.session_id,
        dc.channel_id,
        dd.device_id,
        TO_CHAR(TO_TIMESTAMP(st.visit_time), 'YYYYMMDD')::int4 AS visit_date_id,
        TO_TIMESTAMP(st.visit_time) AS visit_time,
        st.page_views,
        st.session_duration,
        st.clicks,
        st.transactions,
        TO_TIMESTAMP(st.visit_time) AS created_at,
        TO_TIMESTAMP(st.visit_time) AS updated_at
    FROM staging.stg_traffic st
    LEFT JOIN website.dim_channel dc
    ON
        st.traffic_source = dc.traffic_source
        AND st.referral_source = dc.referral_source
    LEFT JOIN website.dim_device dd
    ON 
        st.device = dd.device_name
        AND st.os = dd.os
        AND st.browser = dd.browser
)
SELECT * FROM new_data
ON CONFLICT (session_id) DO UPDATE
SET
    channel_id = EXCLUDED.channel_id,
    device_id = EXCLUDED.device_id,
    visit_date_id = EXCLUDED.visit_date_id,
    visit_time = EXCLUDED.visit_time,
    page_views = EXCLUDED.page_views,
    session_duration = EXCLUDED.session_duration,
    clicks = EXCLUDED.clicks,
    transactions = EXCLUDED.transactions,
    updated_at = EXCLUDED.updated_at
;