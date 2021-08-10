------------------------------------------------------------
-- Test: Shortest path policy APPLICATION
------------------------------------------------------------

DROP TABLE IF EXISTS spp_policy CASCADE;
CREATE UNLOGGED TABLE spp_policy (
       fid integer,
       dest varchar,
       path varchar,
       min_length integer,
       condition TEXT[],
       PRIMARY key (fid)
);
