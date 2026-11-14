-- 1 for a room the server last let us into as a private one: every later join of it asks for a
-- private room, so a room gone meanwhile is not made again as a public one
ALTER TABLE room_state ADD COLUMN private INTEGER NOT NULL DEFAULT 0;
