----------------------------------------------
-- BGP simulation
----------------------------------------------

/* Candidate routing table for BGP simulation -- from RIB files
 * dest: IP prefix
 * path: AS path
 */
DROP TABLE IF EXISTS routes CASCADE;
CREATE UNLOGGED TABLE routes (
       dest TEXT,
       path TEXT
);

/* Routes Updates -- from UPDATE files
 * dest: IP prefix
 * operation: A (Annoucement), W (Withdrawal)
 * path: AS path
 * len_path: length of path
 */
DROP TABLE IF EXISTS routes_delta CASCADE;
CREATE UNLOGGED TABLE routes_delta (
       dest TEXT,
       operation TEXT,
       path TEXT,
       len_path integer
);

