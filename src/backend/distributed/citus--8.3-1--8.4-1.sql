/* citus--8.3-1--8.4-1 */

/* bump version to 8.4-1 */
CREATE OR REPLACE FUNCTION worker_create_if_not_exists(statement text)
  RETURNS bool
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$worker_create_if_not_exists$$;

CREATE OR REPLACE FUNCTION citus_update_dist_object_oids()
  RETURNS bool
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$citus_update_dist_object_oids$$;

CREATE TABLE citus.pg_dist_object (
    classid oid NOT NULL,
    objid oid NOT NULL,
    identifier text NOT NULL
);

GRANT SELECT ON citus.pg_dist_object TO public;
