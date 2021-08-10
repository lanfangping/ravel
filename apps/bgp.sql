------------------------------------------------------------
-- BGP Simulation, policies including:
-- 1. Shortest path policy
-- 2. Static Routing policy
-- 3. filter policy
------------------------------------------------------------

/* Mixed policies
 * dest: IP prefix
 * path: AS path
 * min_len: the minmun of length of AS path
 * condition: condition over variables
 */
DROP TABLE IF EXISTS bgp_policy CASCADE;
CREATE UNLOGGED TABLE bgp_policy (
       dest TEXT,
       path TEXT,
       min_len integer,
       condition TEXT[]
);

/* Violation: */
CREATE OR REPLACE VIEW bgp_violation AS (
       SELECT bgp_policy.dest, routes.dest AS routes_dest,
              bgp_policy.path, routes.path AS routes_dest,
              bgp_policy.min_len, l(routes.path) AS routes_len_path,
              array_cat(bgp_policy.condition, ARRAY[
                            bgp_policy.dest || ' == ' || routes.dest, 
                            bgp_policy.path || ' == ' || routes.path,
                            'l(' || bgp_policy.path || ') == ' || l(routes.path)
                     ]
              ) AS condition 
       FROM bgp_policy, routes 
       WHERE equal(bgp_policy.dest, routes.dest) 
       AND equal(bgp_policy.path, routes.path) 
       AND is_contradiction(condition) 
);

/* Repair */
CREATE OR REPLACE RULE bgp_repair AS
       ON DELETE TO bgp_violation
       DO NOTHING;