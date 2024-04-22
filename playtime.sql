WITH session_durations AS (
    SELECT 
        dt,
        amplitude_id,
        amplitude_session_id,
        TIMESTAMP_DIFF(MAX(client_event_time), MIN(client_event_time), MINUTE) AS session_length
    FROM amplitude.events
    WHERE dt BETWEEN "2024-04-04" AND "2024-04-08"
    GROUP BY 1, 2, 3
),
agg_sessions AS (
    SELECT 
        dt,
        SUM(session_length) AS total_session_length,
        COUNT(DISTINCT amplitude_id) AS user_count
    FROM session_durations
    GROUP BY 1
)
SELECT 
    dt,
    total_session_length / user_count AS avg_daily_playtime_per_user
FROM agg_sessions
ORDER BY 1 ASC;
