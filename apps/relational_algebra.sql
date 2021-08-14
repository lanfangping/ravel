------------------------------------------------------------
-- Relational Algebra Application
------------------------------------------------------------

/* This doesn't do anything interesting, just create a table */
DROP TABLE IF EXISTS relational_algebra CASCADE;
CREATE UNLOGGED TABLE relational_algebra (
       id  integer PRIMARY KEY
);

/* DEMOs - ctables
 * Policy1: Static Routes, Filter
 * dest: IP prefix
 * path:
 * condition:
 */
DROP TABLE IF EXISTS policy1 CASCADE;
CREATE UNLOGGED TABLE policy1(
       dest TEXT,
       path TEXT,
       condition TEXT[]
);
INSERT INTO Policy1 (dest, path, condition) VALUES 
('1.2.3.4','x','{"x == [ABC]"}'),
('y','z','{"y != 1.2.3.5", "y != 1.2.3.4"}');

/* DEMOs - ctables
 * Policy2: Traffic Balancer
 * dest: IP prefix
 * path:
 * condition:
 */
DROP TABLE IF EXISTS Policy2 CASCADE;
create table Policy2 ( 
       dest TEXT, 
       path TEXT, 
       flag TEXT, 
       condition TEXT[]
);
INSERT INTO Policy2 (dest, path, flag, condition) VALUES 
('1.2.3.4','[ABC]', 'u', '{"u == 1"}'),
('5.6.7.8','[ABC]', 'u', '{"u != 1"}'),
('1.2.3.4','[AC]', 'v', '{"v == 1"}'),
('5.6.7.8','[AC]', 'v', '{"v != 1"}');


/* Violation: */
CREATE OR REPLACE VIEW relational_algebra_violation AS (
       SELECT id
       FROM relational_algebra
);

/* Repair */
CREATE OR REPLACE RULE relational_algebra_repair AS
       ON DELETE TO relational_algebra_violation
       DO NOTHING;