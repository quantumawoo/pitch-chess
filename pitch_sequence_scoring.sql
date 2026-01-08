CREATE TABLE IF NOT EXISTS `your_project.your_dataset.user_pitch_events` (
  -- -----------------------------
  -- Identifiers
  -- -----------------------------
  pitch_id STRING NOT NULL,
  sequence_id STRING NOT NULL,

  -- -----------------------------
  -- Pitch order & context
  -- -----------------------------
  pitch_number INT64 NOT NULL,

  balls INT64 NOT NULL,
  strikes INT64 NOT NULL,

  -- -----------------------------
  -- Pitch characteristics
  -- -----------------------------
  pitch_type STRING NOT NULL,   -- e.g. FF, SL, CH, CU
  zone INT64 NOT NULL,          -- 1–14 strike zone grid

  -- -----------------------------
  -- Outcome (optional at input)
  -- -----------------------------
  outcome STRING,               -- swinging_strike, called_strike, foul, in_play_out, in_play_hit, ball

  -- -----------------------------
  -- Metadata
  -- -----------------------------
  user_id STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()

)
PARTITION BY DATE(created_at)
CLUSTER BY sequence_id;


WITH ordered_pitches AS (
  SELECT
    game_pk,
    at_bat_number,
    pitch_number,
    pitch_type,
    zone,
    balls,
    strikes,
    description,

    CONCAT(balls, '-', strikes) AS count,

    LAG(pitch_type) OVER w AS prev_pitch_type,
    LAG(zone)       OVER w AS prev_zone

  FROM `your_project.your_dataset.your_table`
  WINDOW w AS (
    PARTITION BY game_pk, at_bat_number
    ORDER BY pitch_number
  )
),

scored_pitches AS (
  SELECT *,
    -- ---------------------------
    -- 1. Repetition Logic
    -- ---------------------------
    CASE
      WHEN pitch_type = prev_pitch_type AND zone = prev_zone THEN -2
      WHEN pitch_type != prev_pitch_type AND zone = prev_zone THEN  1
      ELSE 0
    END AS repetition_score,

    -- ---------------------------
    -- 2. Count-Based Intent
    -- ---------------------------
    CASE
      WHEN count = '0-0' AND zone NOT BETWEEN 1 AND 9 THEN -1
      WHEN count IN ('0-2','1-2') AND zone NOT BETWEEN 1 AND 9 THEN  1
      WHEN count IN ('3-0','3-1') AND zone BETWEEN 4 AND 6 THEN -2
      ELSE 0
    END AS count_score,

    -- ---------------------------
    -- 3. Pitch Shape Change
    -- ---------------------------
    CASE
      WHEN prev_pitch_type IS NOT NULL
       AND pitch_type != prev_pitch_type THEN 1
      ELSE 0
    END AS shape_score,

    -- ---------------------------
    -- 4. Vertical Eye-Level Change
    -- ---------------------------
    CASE
      WHEN prev_zone IS NOT NULL
       AND (
            (prev_zone BETWEEN 1 AND 3 AND zone BETWEEN 10 AND 14)
         OR (prev_zone BETWEEN 10 AND 14 AND zone BETWEEN 1 AND 3)
       ) THEN 1
      ELSE 0
    END AS eye_level_score,

    -- ---------------------------
    -- 5. Outcome Modifier
    -- ---------------------------
    CASE
      WHEN description IN ('swinging_strike','swinging_strike_blocked') THEN  2
      WHEN description = 'called_strike' THEN 1
      WHEN description = 'foul' THEN 0
      WHEN description LIKE '%out%' THEN 1
      WHEN description LIKE '%hit%' THEN -2
      ELSE 0
    END AS outcome_score

  FROM ordered_pitches
),

final_scoring AS (
  SELECT *,
    repetition_score
  + count_score
  + shape_score
  + eye_level_score
  + outcome_score AS pitch_score
  FROM scored_pitches
)

SELECT
  game_pk,
  at_bat_number,
  COUNT(*) AS pitches_in_sequence,
  SUM(pitch_score) AS sequence_score,

  CASE
    WHEN SUM(pitch_score) >= 4 THEN 'EXCELLENT'
    WHEN SUM(pitch_score) BETWEEN 1 AND 3 THEN 'GOOD'
    WHEN SUM(pitch_score) = 0 THEN 'NEUTRAL'
    WHEN SUM(pitch_score) BETWEEN -3 AND -1 THEN 'POOR'
    ELSE 'BLUNDER'
  END AS sequence_verdict

FROM final_scoring
GROUP BY game_pk, at_bat_number
ORDER BY sequence_score DESC;

