#!/bin/bash
# Bootstrap the *runtime* database role.
#
# This exists because of one Postgres detail: row-level security is skipped for
# superusers and for table owners, so 0002_rls.sql is decoration unless the app
# connects as a plain role that owns nothing. The compose stack therefore uses two
# identities -- $POSTGRES_USER (owner, only used by `plexus migrate`) and this new
# login, which holds DML on tables it does not own.
#
# Runs once per volume, on an empty data directory, before any migration. Tables do
# not exist yet, so access is granted through DEFAULT privileges rather than GRANTs
# on objects; the migrate service creates them afterwards as $POSTGRES_USER.
set -euo pipefail

: "${POSTGRES_DB:=plexus}"
: "${POSTGRES_USER:=plexus_owner}"
: "${PLEXUS_APP_USER:=plexus_app}"
: "${PLEXUS_APP_PASSWORD:?PLEXUS_APP_PASSWORD must be set}"

psql -q -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --no-psqlrc <<SQL
CREATE ROLE ${PLEXUS_APP_USER} LOGIN PASSWORD '${PLEXUS_APP_PASSWORD}';

REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO ${PLEXUS_APP_USER};

ALTER DEFAULT PRIVILEGES FOR ROLE ${POSTGRES_USER} IN SCHEMA public
    REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE ${POSTGRES_USER} IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ${PLEXUS_APP_USER};
SQL

echo "roles: created ${PLEXUS_APP_USER} (non-owner, non-superuser) on ${POSTGRES_DB}"
