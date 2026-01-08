-- Per-pitch scoring + best alternative pitch suggestion (theory-only)
-- Table: `your_project.your_dataset.user_pitch_events`
-- Pitch types supported: FF, SL, CH, CU, CT, SI, FB
-- Zones supported: 1–14 (Statcast-style grid; 1–9 in-zone, 10–14 out-of-zone)

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

-- Score the ACTUAL pitch (split theory vs outcome so alternatives can ignore outcome)
scored_actual AS (
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

    -- 3) Shape change (change pitch type = +1)
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

    -- Outcome modifier (kept separate; alternatives ignore this)
    CASE
      WHEN outcome IN ('swinging_strike','swinging_strike_blocked') THEN  2
      WHEN outcome = 'called_strike' THEN 1
      WHEN outcome IN ('foul','foul_tip') THEN 0
      WHEN outcome = 'ball' THEN 0
      WHEN outcome IN ('in_play_out','force_out','double_play','gidp','triple_play') THEN 1
      WHEN outcome IN ('in_play_hit','error') THEN -2
      WHEN outcome IS NULL THEN 0
      ELSE 0
    END AS outcome_score

  FROM ordered_pitches
),

actual_with_totals AS (
  SELECT
    *,
    (repetition_score + count_score + shape_score + eye_level_score) AS theory_score,
    (repetition_score + count_score + shape_score + eye_level_score + outcome_score) AS pitch_score
  FROM scored_actual
),

-- Candidate alternatives per pitch: 7 pitch types × 14 zones = 98 candidates
candidates AS (
  SELECT
    a.sequence_id,
    a.pitch_id,
    a.pitch_number,
    a.user_id,
    a.count,
    a.prev_pitch_type,
    a.prev_zone,

    a.pitch_type AS actual_pitch_type,
    a.zone       AS actual_zone,

    cand_pitch_type,
    cand_zone,

    -- Differences (for UI messaging)
    IF(cand_pitch_type != a.pitch_type, 1, 0) AS alt_diff_type,
    IF(cand_zone       != a.zone,       1, 0) AS alt_diff_zone

  FROM actual_with_totals a
  CROSS JOIN UNNEST(['FF','SL','CH','CU','CT','SI','FB']) AS cand_pitch_type
  CROSS JOIN UNNEST(GENERATE_ARRAY(1,14)) AS cand_zone
  WHERE NOT (cand_pitch_type = a.pitch_type AND cand_zone = a.zone) -- exclude identical
),

-- Score each candidate alternative using the SAME theory rules, but with cand_* instead of actual
scored_candidates AS (
  SELECT
    c.*,

    -- 1) Repetition rule relative to previous pitch
    CASE
      WHEN c.prev_pitch_type IS NOT NULL AND c.prev_zone IS NOT NULL
       AND c.cand_pitch_type = c.prev_pitch_type
       AND c.cand_zone = c.prev_zone
      THEN -2
      WHEN c.prev_pitch_type IS NOT NULL AND c.prev_zone IS NOT NULL
       AND c.cand_pitch_type != c.prev_pitch_type
       AND c.cand_zone = c.prev_zone
      THEN 1
      ELSE 0
    END AS cand_repetition_score,

    -- 2) Count-based intent (same mapping)
    CASE
      WHEN c.count = '0-0' AND c.cand_zone NOT BETWEEN 1 AND 9 THEN -1
      WHEN c.count IN ('0-2','1-2') AND c.cand_zone NOT BETWEEN 1 AND 9 THEN  1
      WHEN c.count IN ('3-0','3-1') AND c.cand_zone BETWEEN 4 AND 6 THEN -2
      ELSE 0
    END AS cand_count_score,

    -- 3) Shape change relative to previous pitch
    CASE
      WHEN c.prev_pitch_type IS NOT NULL AND c.cand_pitch_type != c.prev_pitch_type THEN 1
      ELSE 0
    END AS cand_shape_score,

    -- 4) Eye-level change relative to previous zone
    CASE
      WHEN c.prev_zone IS NOT NULL
       AND (
            (c.prev_zone BETWEEN 1 AND 3 AND c.cand_zone BETWEEN 10 AND 14)
         OR (c.prev_zone BETWEEN 10 AND 14 AND c.cand_zone BETWEEN 1 AND 3)
       ) THEN 1
      ELSE 0
    END AS cand_eye_level_score

  FROM candidates c
),

ranked_best_alternative AS (
  SELECT
    *,
    (cand_repetition_score + cand_count_score + cand_shape_score + cand_eye_level_score) AS cand_theory_score,

    ROW_NUMBER() OVER (
      PARTITION BY sequence_id, pitch_id
      ORDER BY
        (cand_repetition_score + cand_count_score + cand_shape_score + cand_eye_level_score) DESC,
        -- tie-breakers: prefer changing BOTH type and zone, then either
        (alt_diff_type + alt_diff_zone) DESC,
        alt_diff_type DESC,
        alt_diff_zone DESC,
        cand_pitch_type,
        cand_zone
    ) AS rn
  FROM scored_candidates
)

SELECT
  a.sequence_id,
  a.user_id,
  a.pitch_id,
  a.pitch_number,
  a.count,

  a.pitch_type,
  a.zone,
  a.outcome,

  -- Scores
  a.repetition_score,
  a.count_score,
  a.shape_score,
  a.eye_level_score,
  a.outcome_score,
  a.theory_score,
  a.pitch_score,

  -- Best alternative (theory-only)
  b.cand_pitch_type AS best_alt_pitch_type,
  b.cand_zone       AS best_alt_zone,
  b.cand_theory_score AS best_alt_theory_score,
  b.alt_diff_type,
  b.alt_diff_zone

FROM actual_with_totals a
LEFT JOIN ranked_best_alternative b
  ON a.sequence_id = b.sequence_id
 AND a.pitch_id    = b.pitch_id
 AND b.rn = 1

ORDER BY a.sequence_id, a.pitch_number;
