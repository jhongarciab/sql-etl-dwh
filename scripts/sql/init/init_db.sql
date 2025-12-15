-- ============================================================
-- INIT DATA WAREHOUSE - PostgreSQL
-- Arquitectura Medallion: Bronze / Silver / Gold
-- Este archivo NO contiene usuarios ni contraseñas
-- ============================================================

-- ------------------------------------------------------------
-- 1. Cerrar conexiones activas a la base (si existe)
-- (Ejecutar conectado a la base "postgres")
-- ------------------------------------------------------------
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'datawarehouse'
  AND pid <> pg_backend_pid();

-- ------------------------------------------------------------
-- 2. Eliminar y crear la base de datos
-- ------------------------------------------------------------
DROP DATABASE IF EXISTS datawarehouse;
CREATE DATABASE datawarehouse;

-- ------------------------------------------------------------
-- 3. Conectarse a la nueva base
-- (Este comando funciona SOLO en psql.
--  En pgAdmin debes abrir el Query Tool sobre datawarehouse)
-- ------------------------------------------------------------
\c datawarehouse

-- ------------------------------------------------------------
-- 4. Crear esquemas Medallion
-- ------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ------------------------------------------------------------
-- 5. Verificación (opcional)
-- ------------------------------------------------------------
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('bronze', 'silver', 'gold')
ORDER BY schema_name;
