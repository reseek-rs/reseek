-- how many turns each wishlist item has had, and the turn that last found each file it keeps, so
-- the files found longest ago go first once an item keeps too many
ALTER TABLE wishlist ADD COLUMN turns INTEGER NOT NULL DEFAULT 0;
ALTER TABLE wishlist_seen ADD COLUMN last_turn INTEGER NOT NULL DEFAULT 0;
CREATE INDEX wishlist_seen_turn ON wishlist_seen (item_id, last_turn);
