/* citus--8.3-1--8.4-1 */

/* bump version to 8.4-1 */
CREATE OR REPLACE FUNCTION worker_create_if_not_exists(statement text)
  RETURNS bool
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$worker_create_if_not_exists$$;
