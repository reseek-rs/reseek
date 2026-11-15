-- the password each local account had when the authentication options last stood, so a
-- password changed while the daemon was stopped is found at the next start: the configured hash
-- as it is, or an argon2 hash of a configured plain password, its salt in the hash
CREATE TABLE passwords (
  subject TEXT PRIMARY KEY,
  fingerprint TEXT NOT NULL,
  changed_at TEXT NOT NULL
);

-- the browser sessions that set a local account's password since the options last stood, so
-- the sessions a new password ends keep the one that set it, across a restart too
CREATE TABLE password_setters (
  session_id TEXT NOT NULL,
  subject TEXT NOT NULL,
  account TEXT NOT NULL
);
