-- Connect to the PostgreSQL server as a superuser
-- For example, using psql:
-- psql -U postgres

-- Step 1: Create the database
CREATE DATABASE envio;

-- Step 2: Create the user
CREATE USER envio WITH PASSWORD 'your_password';

-- Step 3: Grant privileges to the user
GRANT ALL PRIVILEGES ON DATABASE envio TO envio;

-- Step 4: Connect to the newly created database
\c envio

-- Step 5: Grant the user the ability to create tables
GRANT CREATE ON SCHEMA public TO envio;
