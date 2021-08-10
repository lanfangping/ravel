------------------------------------------------------------
-- Test: Shortest path policy APPLICATION
------------------------------------------------------------

DROP TABLE IF EXISTS test_policy CASCADE;
CREATE UNLOGGED TABLE test_policy (
       fid integer,
       dest varchar,
       path varchar,
       min_length integer,
       condition TEXT[],
       PRIMARY key (fid)
);
