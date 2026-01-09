-- Adds Statcast batter context to user pitch events
-- Allows Statcast-based modifiers while preserving pure-theory mode

ALTER TABLE `your_project.your_dataset.user_pitch_events`
ADD COLUMN batter INT64;
