------------------------------------------------------------
-- Relational Algebra Application
------------------------------------------------------------

/* This doesn't do anything interesting, just create a table */
DROP TABLE IF EXISTS relational_algebra CASCADE;
CREATE UNLOGGED TABLE relational_algebra (
       id  integer PRIMARY KEY
);

/* Violation: */
CREATE OR REPLACE VIEW relational_algebra_violation AS (
       SELECT id
       FROM relational_algebra
);

/* Repair */
CREATE OR REPLACE RULE relational_algebra_repair AS
       ON DELETE TO relational_algebra_violation
       DO NOTHING;