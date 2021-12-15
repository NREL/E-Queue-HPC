WITH t AS 
    (SELECT queue::SMALLINT, priority::int, id::uuid, parent::uuid, depth::SMALLINT, command::jsonb FROM 
        (VALUES  (-1,3037,'30fe8c59-4985-4045-8ea7-f1465f9b16ec'::uuid, NULL ,0,'{"id":1}') )
         AS t (queue, priority, id, parent, depth, command)),
  s AS (INSERT INTO test_job_status (queue, priority, id)
                    (SELECT queue, priority, id FROM t)
                    ON CONFLICT DO NOTHING)
INSERT INTO test_job_data (id, parent, depth, command)
    (SELECT id, parent, depth, command FROM t)
    ON CONFLICT DO NOTHING;



SELECT * FROM test_job_status;