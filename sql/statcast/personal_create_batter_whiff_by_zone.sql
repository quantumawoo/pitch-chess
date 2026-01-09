CREATE OR REPLACE TABLE `catcher-analysis-483801.pitch_chess.batter_whiff_by_zone` AS
WITH pitch_events AS (
  SELECT
    batter,
    zone,
    description
  FROM `catcher-analysis-483801.pitch_chess.statcast_pitches`
  WHERE batter IS NOT NULL
    AND zone IS NOT NULL
    AND zone BETWEEN 1 AND 14
),

labeled AS (
  SELECT
    batter,
    zone,
    CASE
      WHEN description IN (
        'swinging_strike',
        'swinging_strike_blocked',
        'foul',
        'foul_tip',
        'in_play',
        'foul_bunt'
      ) THEN 1 ELSE 0
    END AS is_swing,

    CASE
      WHEN description IN (
        'swinging_strike',
        'swinging_strike_blocked'
      ) THEN 1 ELSE 0
    END AS is_whiff
  FROM pitch_events
)

SELECT
  batter,
  zone,
  SUM(is_whiff) AS whiffs,
  SUM(is_swing) AS swings,
  SAFE_DIVIDE(SUM(is_whiff), SUM(is_swing)) AS whiff_pct
FROM labeled
GROUP BY batter, zone
HAVING swings >= 30;

