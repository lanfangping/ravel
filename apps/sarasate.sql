------------------------------------------------------------
-- Relational Algebra Application
------------------------------------------------------------

/* This doesn't do anything interesting, just create a table */
DROP TABLE IF EXISTS sarasate CASCADE;
CREATE UNLOGGED TABLE sarasate (
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

/* Instance 1 */
DROP TABLE IF EXISTS instance1 CASCADE;
create table instance1 ( DEST TEXT, PATH TEXT,CONDITION TEXT []);
insert into instance1 ( DEST,PATH, CONDITION) values 
('1.2.3.4','[ABC]','{"[ABC] == [ABC]"}'),
('5.6.7.8','[AC]','{"5.6.7.8 != 1.2.3.5", "5.6.7.8 != 1.2.3.4"}');

/* Instance 2: contradictary*/
DROP TABLE IF EXISTS instance2 CASCADE;
create table instance2 ( DEST TEXT, PATH TEXT,CONDITION TEXT []);
insert into instance2 ( DEST,PATH, CONDITION) values 
('1.2.3.4','[AC]','{"[AC] == [ABC]"}'),
('1.2.3.5','[ADC]','{"1.2.3.5 != 1.2.3.5", "1.2.3.5 != 1.2.3.4"}');

/* Violation: */
CREATE OR REPLACE VIEW sarasate_violation AS (
       SELECT id
       FROM sarasate
);

/* Repair */
CREATE OR REPLACE RULE sarasate_repair AS
       ON DELETE TO sarasate_violation
       DO NOTHING;