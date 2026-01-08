WITH ordered_pitches AS (
  SELECT
    sequence_id,
    user_id,
    pitch_id,
    pitch_number,
    pitch_type,
    zone,
    balls,
    strikes,
    outcome,
    created_at,

    CONCAT(CAST(balls AS STRING), '-', CAST(strikes AS STRING)) AS count,

    LAG(pitch_type) OVER w AS prev_pitch_type,
    LAG(zone)       OVER w AS prev_zone

  FROM `your_project.your_dataset.user_pitch_events`
  WINDOW w AS (
    PARTITION BY sequence_id
    ORDER BY pitch_number
  )
),

scored_pitches AS (
  SELECT
    *,

    -- 1) No exact repetition (type+zone)
    CASE
      WHEN pitch_type = prev_pitch_type AND zone = prev_zone THEN -2
      WHEN pitch_type != prev_pitch_type AND zone = prev_zone THEN  1
      ELSE 0
    END AS repetition_score,

    -- 2) Count-based intent (MVP)
    CASE
      WHEN count = '0-0' AND zone NOT BETWEEN 1 AND 9 THEN -1
      WHEN count IN ('0-2','1-2') AND zone NOT BETWEEN 1 AND 9 THEN  1
      WHEN count IN ('3-0','3-1') AND zone BETWEEN 4 AND 6 THEN -2
      ELSE 0
    END AS count_score,

    -- 3) Shape change (very simplified: change pitch type = +1)
    CASE
      WHEN prev_pitch_type IS NOT NULL AND pitch_type != prev_pitch_type THEN 1
      ELSE 0
    END AS shape_score,

    -- 4) Eye-level change (high↔low)
    CASE
      WHEN prev_zone IS NOT NULL
       AND (
            (prev_zone BETWEEN 1 AND 3 AND zone BETWEEN 10 AND 14)
         OR (prev_zone BETWEEN 10 AND 14 AND zone BETWEEN 1 AND 3)
       ) THEN 1
      ELSE 0
    END AS eye_level_score,

    -- 5) Outcome modifier (uses your `outcome` column)
    CASE
      WHEN outcome IN ('swinging_strike','swinging_strike_blocked') THEN  2
      WHEN outcome = 'called_strike' THEN 1
      WHEN outcome IN ('foul','foul_tip') THEN 0
      WHEN outcome = 'ball' THEN 0
      WHEN outcome IN ('in_play_out','force_out','double_play','gidp','triple_play') THEN 1
      WHEN outcome IN ('in_play_hit','error') THEN -2
      WHEN outcome IS NULL THEN 0  -- allow “theory only” sequences
      ELSE 0
    END AS outcome_score

  FROM ordered_pitches
),

final_scoring AS (
  SELECT
    *,
    (repetition_score + count_score + shape_score + eye_level_score + outcome_score) AS pitch_score
  FROM scored_pitches
)

-- Output 1: per-pitch detail (useful for UI)
SELECT
  sequence_id,
  user_id,
  pitch_id,
  pitch_number,
  count,
  pitch_type,
  zone,
  outcome,

  repetition_score,
  count_score,
  shape_score,
  eye_level_score,
  outcome_score,

  pitch_score
FROM final_scoring
ORDER BY sequence_id, pitch_number;
