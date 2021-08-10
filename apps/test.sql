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

-- CREATE OR REPLACE VIEW test_violation AS (
--        SELECT rm.fid, rate AS req, vol AS asgn
--        FROM rm, test_policy
--        WHERE rm.fid = test_policy.fid AND rate > vol
-- );