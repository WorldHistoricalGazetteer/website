#!/bin/bash
set -e

echo "Running local_load_database.sh ..."

database_exists() {
  psql -U "$POSTGRES_USER" -lqt | cut -d \| -f 1 | grep -qw "$1"
}

# Ensure the main user role exists
psql -U "$POSTGRES_USER" -c "DO \$\$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$DB_USER') THEN
      CREATE ROLE $DB_USER WITH LOGIN PASSWORD '$DB_PASSWORD' SUPERUSER CREATEDB;
   ELSE
      RAISE NOTICE 'Role $DB_USER already exists';
   END IF;
END
\$\$;"
echo "Default superuser $DB_USER ensured."

# Ensure the read-only role exists
psql -U "$POSTGRES_USER" -c "DO \$\$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'ro_user') THEN
      CREATE ROLE ro_user WITH LOGIN NOINHERIT;
   ELSE
      RAISE NOTICE 'Role ro_user already exists';
   END IF;
END
\$\$;"
echo "Read-only user 'ro_user' ensured."

# Check if the database exists
if database_exists $DB_NAME; then
 dropdb -U "$POSTGRES_USER" "$DB_NAME"
fi

# Create the database
createdb -U "$POSTGRES_USER" "$DB_NAME"
echo "Database $DB_NAME created."

# Load data from the dump file
echo "Loading data from dump file..."
#gunzip -c /app/data/base_data.sql.gz | psql -U "$POSTGRES_USER" -d "$DB_NAME"
gunzip -c /app/data/whgv3beta_20241025_0100_short_term.backup | psql -U "$POSTGRES_USER" -d "$DB_NAME"
echo "Data loaded successfully."

# Grant read-only permissions to ro_user
psql -U "$POSTGRES_USER" -d "$DB_NAME" -c "GRANT CONNECT ON DATABASE $DB_NAME TO ro_user;"
psql -U "$POSTGRES_USER" -d "$DB_NAME" -c "GRANT USAGE ON SCHEMA public TO ro_user;"
psql -U "$POSTGRES_USER" -d "$DB_NAME" -c "GRANT SELECT ON ALL TABLES IN SCHEMA public TO ro_user;"

echo "Permissions granted to read-only user 'ro_user'."
