WITH ordered_pitches AS (
  SELECT
    sequence_id,
    user_id,
    batter,
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

  FROM `catcher-analysis-483801.pitch_chess.user_pitch_events`
  WINDOW w AS (
    PARTITION BY sequence_id
    ORDER BY pitch_number
  )
),

scored_actual AS (
  SELECT
    *,

    CASE
      WHEN pitch_type = prev_pitch_type AND zone = prev_zone THEN -2
      WHEN pitch_type != prev_pitch_type AND zone = prev_zone THEN  1
      ELSE 0
    END AS repetition_score,

    CASE
      WHEN count = '0-0' AND zone NOT BETWEEN 1 AND 9 THEN -1
      WHEN count IN ('0-2','1-2') AND zone NOT BETWEEN 1 AND 9 THEN  1
      WHEN count IN ('3-0','3-1') AND zone BETWEEN 4 AND 6 THEN -2
      ELSE 0
    END AS count_score,

    CASE
      WHEN prev_pitch_type IS NOT NULL AND pitch_type != prev_pitch_type THEN 1
      ELSE 0
    END AS shape_score,

    CASE
      WHEN prev_zone IS NOT NULL
       AND (
            (prev_zone BETWEEN 1 AND 3 AND zone BETWEEN 10 AND 14)
         OR (prev_zone BETWEEN 10 AND 14 AND zone BETWEEN 1 AND 3)
       ) THEN 1
      ELSE 0
    END AS eye_level_score,

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

candidates AS (
  SELECT
    a.sequence_id,
    a.pitch_id,
    a.pitch_number,
    a.user_id,
    a.batter,
    a.count,
    a.prev_pitch_type,
    a.prev_zone,

    a.pitch_type AS actual_pitch_type,
    a.zone       AS actual_zone,

    cand_pitch_type,
    cand_zone,

    IF(cand_pitch_type != a.pitch_type, 1, 0) AS alt_diff_type,
    IF(cand_zone       != a.zone,       1, 0) AS alt_diff_zone

  FROM actual_with_totals a
  CROSS JOIN UNNEST(['FF','SL','CH','CU','CT','SI','FB']) AS cand_pitch_type
  CROSS JOIN UNNEST(GENERATE_ARRAY(1,14)) AS cand_zone
  WHERE NOT (cand_pitch_type = a.pitch_type AND cand_zone = a.zone)
),

scored_candidates AS (
  SELECT
    c.*,

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

    CASE
      WHEN c.count = '0-0' AND c.cand_zone NOT BETWEEN 1 AND 9 THEN -1
      WHEN c.count IN ('0-2','1-2') AND c.cand_zone NOT BETWEEN 1 AND 9 THEN  1
      WHEN c.count IN ('3-0','3-1') AND c.cand_zone BETWEEN 4 AND 6 THEN -2
      ELSE 0
    END AS cand_count_score,

    CASE
      WHEN c.prev_pitch_type IS NOT NULL AND c.cand_pitch_type != c.prev_pitch_type THEN 1
      ELSE 0
    END AS cand_shape_score,

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
        (alt_diff_type + alt_diff_zone) DESC,
        alt_diff_type DESC,
        alt_diff_zone DESC,
        cand_pitch_type,
        cand_zone
    ) AS rn
  FROM scored_candidates
),

best_alt AS (
  SELECT
    *,
    CASE
      WHEN alt_diff_type = 1 AND alt_diff_zone = 1 THEN 'Change pitch type and location'
      WHEN alt_diff_type = 1 AND alt_diff_zone = 0 THEN 'Change pitch type'
      WHEN alt_diff_type = 0 AND alt_diff_zone = 1 THEN 'Change location'
      ELSE 'Adjust pitch selection'
    END AS best_alt_change_recommendation,

    -- Primary reason: pick the strongest single “principle” the alternative achieves
    CASE
      WHEN cand_repetition_score = 1 THEN 'Avoid predictability: same location works better with a different pitch type'
      WHEN cand_repetition_score = -2 THEN 'Avoid predictability: do not repeat exact pitch type and location'
      WHEN cand_count_score = 1 THEN 'Count leverage: expand the zone to chase'
      WHEN cand_count_score = -1 THEN 'Early count: establish a competitive strike'
      WHEN cand_count_score = -2 THEN 'Hitter’s count: avoid the danger middle'
      WHEN cand_eye_level_score = 1 THEN 'Disrupt the hitter’s eye level (high–low change)'
      WHEN cand_shape_score = 1 THEN 'Disrupt timing with a different pitch shape'
      ELSE 'Higher-theory option based on sequencing principles'
    END AS best_alt_reason_primary,

    -- Details: show which components improved (compact, UI-friendly)
    ARRAY_TO_STRING(
      ARRAY(
        SELECT reason FROM UNNEST([
          IF(cand_repetition_score = 1, 'Improves repetition rule', NULL),
          IF(cand_count_score != 0, 'Better count intent', NULL),
          IF(cand_shape_score = 1, 'Better pitch-shape change', NULL),
          IF(cand_eye_level_score = 1, 'Better eye-level change', NULL)
        ]) AS reason
        WHERE reason IS NOT NULL
      ),
      '; '
    ) AS best_alt_reason_details

  FROM ranked_best_alternative
  WHERE rn = 1
)

SELECT
  a.sequence_id,
  a.user_id,
  a.batter,
  a.pitch_id,
  a.pitch_number,
  a.count,

  a.pitch_type,
  a.zone,
  a.outcome,

  a.theory_score,
  a.pitch_score,

  b.cand_pitch_type AS best_alt_pitch_type,
  b.cand_zone       AS best_alt_zone,
  b.cand_theory_score AS best_alt_theory_score,

  b.best_alt_change_recommendation,
  b.best_alt_reason_primary,
  b.IFNULL(b.best_alt_reason_details, '') AS best_alt_reason_details

FROM actual_with_totals a
LEFT JOIN best_alt b
  ON a.sequence_id = b.sequence_id
 AND a.pitch_id    = b.pitch_id
 AND b.cand_theory_score > a.theory_score
ORDER BY a.sequence_id, a.pitch_number;
