-- Adds Statcast batter context to user pitch events
-- Allows Statcast-based modifiers while preserving pure-theory mode

ALTER TABLE `catcher-analysis-483801.pitch_chess.user_pitch_events`
ADD COLUMN batter INT64;
