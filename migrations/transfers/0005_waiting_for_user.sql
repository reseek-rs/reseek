-- whether a download waits here for its user, who is offline, to come back; cleared whenever it
-- moves on. The downloads written before this said so by their reason alone, in state 2050,
-- `Queued, Locally`
ALTER TABLE Transfers ADD COLUMN WaitingForUser INTEGER NOT NULL DEFAULT 0;
UPDATE Transfers SET WaitingForUser = 1
WHERE Direction = 'Download' AND State = 2050 AND Paused = 0 AND Exception = 'The user is offline';
