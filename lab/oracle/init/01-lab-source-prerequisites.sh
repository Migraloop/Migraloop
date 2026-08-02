#!/bin/bash
# Lab Oracle Source Prerequisites for LogMiner capture (ADR-0021 / issue #59).
# Runs once on first container init as the oracle image's entrypoint user.
set -euo pipefail

echo "Lab: enabling ARCHIVELOG, FRA, and database supplemental logging"

sqlplus -s / as sysdba <<'SQL'
WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SYSTEM SET db_recovery_file_dest_size=5G SCOPE=BOTH;
ALTER SYSTEM SET db_recovery_file_dest='/opt/oracle/oradata' SCOPE=BOTH;
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
-- CDB-level minimum supplemental logging
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
-- Ensure Lab PDB is open and has DB supplemental logging for LogMiner
ALTER PLUGGABLE DATABASE ALL OPEN;
ALTER SESSION SET CONTAINER=FREEPDB1;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
EXIT;
SQL

echo "Lab: granting SYNC_USER LogMiner / capture privileges in FREEPDB1"

sqlplus -s / as sysdba <<'SQL'
WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SESSION SET CONTAINER=FREEPDB1;
-- APP_USER (SYNC_USER) is created by the image; widen for Lab capture path.
BEGIN
  EXECUTE IMMEDIATE 'GRANT LOGMINING TO SYNC_USER';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -1917 THEN RAISE; END IF; -- ignore if role/priv missing on edition
END;
/
GRANT SELECT_CATALOG_ROLE TO SYNC_USER;
GRANT EXECUTE_CATALOG_ROLE TO SYNC_USER;
GRANT SELECT ANY TRANSACTION TO SYNC_USER;
GRANT CREATE SESSION TO SYNC_USER;
GRANT ALTER SESSION TO SYNC_USER;
-- Lab Fixture does not create sample business tables; Scenarios own Namespace DDL.
-- Allow SYNC_USER to create its own schema objects for later Scenario Namespace use.
GRANT CREATE TABLE TO SYNC_USER;
GRANT UNLIMITED TABLESPACE TO SYNC_USER;
-- Dictionary / redo views commonly needed for LogMiner probes
BEGIN
  FOR v IN (
    SELECT column_value AS view_name FROM TABLE(sys.odcivarchar2list(
      'V_$DATABASE', 'V_$ARCHIVED_LOG', 'V_$LOG', 'V_$LOGFILE',
      'V_$LOGMNR_CONTENTS', 'V_$PARAMETER'
    ))
  ) LOOP
    BEGIN
      EXECUTE IMMEDIATE 'GRANT SELECT ON ' || v.view_name || ' TO SYNC_USER';
    EXCEPTION
      WHEN OTHERS THEN NULL;
    END;
  END LOOP;
END;
/
EXIT;
SQL

echo "Lab: Source Prerequisites init complete"
