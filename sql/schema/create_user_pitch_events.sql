CREATE TABLE IF NOT EXISTS `catcher-analysis-483801.pitch_chess.user_pitch_events` (
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


