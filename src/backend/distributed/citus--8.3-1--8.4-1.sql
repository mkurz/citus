/* citus--8.3-1--8.4-1 */

/* bump version to 8.4-1 */
CREATE OR REPLACE FUNCTION worker_create_if_not_exists(statement text)
  RETURNS bool
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$worker_create_if_not_exists$$;

  CREATE OR REPLACE FUNCTION worker_create_or_replace(statement text)
  RETURNS bool
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$worker_create_or_replace$$;

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

CREATE OR REPLACE FUNCTION pg_catalog.citus_drop_trigger()
    RETURNS event_trigger
    LANGUAGE plpgsql
    SET search_path = pg_catalog
    AS $cdbdt$
DECLARE
    v_obj record;
    sequence_names text[] := '{}';
    table_colocation_id integer;
    propagate_drop boolean := false;
BEGIN
    -- collect set of dropped sequences to drop on workers later
    SELECT array_agg(object_identity) INTO sequence_names
    FROM pg_event_trigger_dropped_objects()
    WHERE object_type = 'sequence';

    FOR v_obj IN SELECT * FROM pg_event_trigger_dropped_objects()
                 WHERE object_type IN ('table', 'foreign table')
    LOOP
        -- first drop the table and metadata on the workers
        -- then drop all the shards on the workers
        -- finally remove the pg_dist_partition entry on the coordinator
        PERFORM master_remove_distributed_table_metadata_from_workers(v_obj.objid, v_obj.schema_name, v_obj.object_name);
        PERFORM master_drop_all_shards(v_obj.objid, v_obj.schema_name, v_obj.object_name);
        PERFORM master_remove_partition_metadata(v_obj.objid, v_obj.schema_name, v_obj.object_name);
    END LOOP;

    IF cardinality(sequence_names) > 0 THEN
        PERFORM master_drop_sequences(sequence_names);
    END IF;

    -- remove entries from citus.pg_dist_object for all dropped objects
    FOR v_obj IN SELECT * FROM pg_event_trigger_dropped_objects() AS drop_object
                          JOIN citus.pg_dist_object AS dist_object
                            ON (dist_object.classid = drop_object.classid
                                AND dist_object.objid = drop_object.objid
                                AND drop_object.objsubid = 0)
    LOOP
        DELETE FROM citus.pg_dist_object
              WHERE classid = v_obj.classid
                AND objid = v_obj.objid;
    END LOOP;

END;
$cdbdt$;
COMMENT ON FUNCTION pg_catalog.citus_drop_trigger()
    IS 'perform checks and actions at the end of DROP actions';
