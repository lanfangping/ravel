
/* Violation: */
CREATE OR REPLACE VIEW relational_algebra_violation;

/* Repair */
CREATE OR REPLACE RULE bgp_repair AS
       ON DELETE TO bgp_violation
       DO NOTHING;