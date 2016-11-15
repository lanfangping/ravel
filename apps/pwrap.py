import os
import sys
import importlib
from ipaddr import IPv4Network

from psycopg2.extensions import AsIs

from ravel.util import Config
from ravel.app import AppConsole

# http://stackoverflow.com/questions/15178859/postgres-constraint-ensuring-one-column-of-many-is-present

# TODO:
# how to determine sid?
# handle nexthop view when port/nexthop doesn't exist
# enforce (max of a) single action
sql = """
------------------------------------------------------------
-- POLICY TABLE
------------------------------------------------------------
DROP TABLE IF EXISTS pyr_{0}_policy CASCADE;
CREATE UNLOGGED TABLE pyr_{0}_policy (
    id       integer PRIMARY KEY,
    sid      integer,

    -- Matches
    dstip    varchar(16),
    srcip    varchar(16),
    dstmac   varchar(17),
    srcmac   varchar(17),
    inport   integer,
    dltype   integer,

    -- Rewrite
    rewrite        boolean,
    rewrite_srcip  varchar(16),
    rewrite_dstip  varchar(16),

    -- Actions
    drop boolean,
    fwd integer,
    xfwd integer
);



------------------------------------------------------------
-- PYRETIC POLICY <--> RAVEL OBJECT MAPPINGS
------------------------------------------------------------
-- Map rm to to header fields (ip, mac)
CREATE OR REPLACE VIEW rm_match AS (
SELECT rm.fid,
       src.ip AS srcip,
       src.mac AS srcmac,
       dst.ip AS dstip,
       dst.mac AS dstmac
  FROM rm
  LEFT JOIN hosts dst
    ON dst.hid = rm.dst
  LEFT JOIN hosts src
    ON src.hid = rm.src
);

-- Map pyretic policy to node IDs
CREATE OR REPLACE VIEW pyr_{0}_translation AS (
    SELECT p.id,
           src.hid AS src,
           dst.hid AS dst,
           rewrite,
           drop
    FROM pyr_{0}_policy p
    LEFT JOIN hosts src
      ON src.ip = p.srcip
    LEFT JOIN hosts dst
      ON dst.ip = p.dstip
);



------------------------------------------------------------
-- POLICY ACTION-SPECIFIC VIEWS
------------------------------------------------------------
CREATE OR REPLACE VIEW pyr_{0}_drops AS (
    SELECT src, dst
    FROM pyr_{0}_translation
    WHERE drop = true
);

CREATE OR REPLACE VIEW pyr_{0}_srcip_rewrites AS (
    SELECT srcip, rewrite_srcip
    FROM pyr_{0}_policy
    WHERE rewrite = true AND rewrite_srcip != ''
);

CREATE OR REPLACE VIEW pyr_{0}_dstip_rewrites AS (
    SELECT dstip, rewrite_dstip
    FROM pyr_{0}_policy
    WHERE rewrite = true AND rewrite_dstip != ''
);



------------------------------------------------------------
-- VIOLATIONS
------------------------------------------------------------
CREATE OR REPLACE VIEW pyr_{0}_drop_violation AS (
    SELECT fid
    FROM rm
    WHERE (src, dst) IN (SELECT src, dst FROM pyr_{0}_drops)
);

CREATE OR REPLACE VIEW pyr_{0}_rewrite_srcip_violation AS (
    SELECT fid
    FROM rm_match
    WHERE srcip IN (SELECT srcip FROM pyr_{0}_srcip_rewrites)
);

CREATE OR REPLACE VIEW pyr_{0}_rewrite_dstip_violation AS (
    SELECT fid
    FROM rm_match
    WHERE dstip IN (SELECT dstip FROM pyr_{0}_dstip_rewrites)
);

CREATE OR REPLACE VIEW pyr_{0}_rewrite_violation AS (
    SELECT * FROM pyr_{0}_rewrite_srcip_violation
        UNION
     SELECT * from pyr_{0}_rewrite_dstip_violation
);

-- Application's violation view: union of each action violation
CREATE OR REPLACE VIEW pyr_{0}_violation AS (
    SELECT fid FROM pyr_{0}_drop_violation
        UNION
    SELECT fid FROM pyr_{0}_rewrite_violation
);



------------------------------------------------------------
-- REPAIR
------------------------------------------------------------
CREATE OR REPLACE RULE pyr_{0}_repair AS
   ON DELETE TO pyr_{0}_violation
   DO INSTEAD (
       DELETE FROM pyr_{0}_drop_violation WHERE fid = OLD.fid;
       DELETE FROM pyr_{0}_rewrite_violation WHERE fid = OLD.fid;
   );

-- Drop repair - remove the violating entry from rm
CREATE OR REPLACE RULE pyr_{0}_drop_repair AS
    ON DELETE TO pyr_{0}_drop_violation
    DO INSTEAD
        DELETE FROM rm WHERE fid = OLD.fid;
"""

cleanup = """
DROP TABLE IF EXISTS pyr_{0}_policy CASCADE;
DROP VIEW IF EXISTS pyr_{0}_translation CASCADE;
DROP VIEW IF EXISTS pyr_{0}_drops CASCADE;
DROP VIEW IF EXISTS pyr_{0}_srcip_rewrites CASCADE;
DROP VIEW IF EXISTS pyr_{0}_dstip_rewrites CASCADE;
DROP VIEW IF EXISTS pyr_{0}_drop_violation CASCADE;
DROP VIEW IF EXISTS pyr_{0}_rewrite_srcip_violation CASCADE;
DROP VIEW IF EXISTS pyr_{0}_rewrite_dstip_violation CASCADE;
DROP VIEW IF EXISTS pyr_{0}_rewrite_violation CASCADE;
DROP VIEW IF EXISTS pyr_{0}_violation CASCADE;
"""

def updatePath():
    path = ""
    if 'PYTHONPATH' in os.environ:
        path = os.environ['PYTHONPATH']

    sys.path = path.split(':') + sys.path
    cwd = os.path.dirname(os.path.abspath(__file__))

    # add pyretic wrapper, apps
    pyreticpath = os.path.normpath(os.path.join(cwd, "pyretic_apps"))
    sys.path.append(os.path.abspath(pyreticpath))

    # add pox
    sys.path.append(Config.PoxDir)

class ViolationCondition(object):
    def __init__(match, name=None):
        self.match = match
        self.name = name

    def to_string(self):
        from ipaddr import IPv4Network
        col = {}
        for field, value in self.match.map.iteritems():
            if isinstance(value, IPv4Network):
                if value.prefixlen != 32:
                    print "Can't handle prefixes < 32"
                    break
                ip  = str(value)[:-3]
                col[field] = "'{0}'".format(ip)

        query = "AND".join("{0} = {1}".format(k,v) for k,v in col.iteritems())
        view = "CREATE OR REPLACE VIEW {0}_violation AS (\n  SELECT fid FROM rm_match\n  WHERE "
        view += query + ";"
        view += "\n);"

        if self.name is not None:
            view = view.format(self.name)

        return view


opcount = 0
def parse_policy(policy, appname):
    global opcount
    opcount = 0

    views, repairs = parse_policy_rec(policy, appname)
    result = ""
    result += "\n".join(vw for _,vw in views)
    result += "\n"
    result += "\n".join(r for r in repairs)

    # for each top-level (composed in parallel) view, create union view
    result += "\nCREATE OR REPLACE VIEW {0}_violation AS (\n".format(appname)
    result += "    UNION\n".join("  SELECT fid FROM {0}\n".format(viewname) for viewname,_ in views)
    result += ");"

    # for each top-level view, create app repair rule
    result += "\nCREATE OR REPLACE RULE {0}_repair AS (\n".format(appname)
    result += "  ON DELETE TO {0}_violation\n".format(appname)
    result += "  DO INSTEAD (\n"
    result += "\n".join("    DELETE FROM {0}_violation;".format(viewname) for viewname,_ in views)
    result += "\n);"
    return result

def parse_policy_rec(policy, appname, views=None, repairs=None):
    from pyretic.core.language import CombinatorPolicy, union, sequential, \
        negate, match, if_, drop, parallel, modify

    global opcount
    if not views:
        views = []
    if not repairs:
        repairs = []

    if hasattr(policy, 'policies'):
        for subpol in policy.policies:
            v, r = parse_policy_rec(subpol, appname)
            views.extend(v)
            repairs.extend(r)

    opname = "{0}_op{1}".format(appname, opcount)
    if isinstance(policy, sequential):
        opcount += 1
        return (views, repairs)
    elif isinstance(policy, parallel):
        opcount += 1
        return (views, repairs)
    elif isinstance(policy, match):
        matchobj = parse_match(policy)
        matchobj = matchobj.format(opname)
        views.append((opname, matchobj))
    elif isinstance(policy, modify):
        modifyobj = parse_modify(policy)
        modifyobj = modifyobj.format(opname)
        repairs.append(modifyobj)

    return (views, repairs)

def parse_match(match):
    from ipaddr import IPv4Network
    col = {}
    for field, value in match.map.iteritems():
        if isinstance(value, IPv4Network):
            if value.prefixlen != 32:
                print "Can't handle prefixes < 32"
                break
            ip  = str(value)[:-3]
            col[field] = "'{0}'".format(ip)

    query = "AND".join("{0} = {1}".format(k,v) for k,v in col.iteritems())
    view = "CREATE OR REPLACE VIEW {0}_violation AS (\n  SELECT fid FROM rm_match\n  WHERE "
    view += query + ";"
    view += "\n);"
    return view

def parse_modify(modify):
    from pyretic.core.network import IPAddr
    col = {}
    for k,v in modify.map.iteritems():
        ip = None
        if isinstance(v, IPv4Network):
            if v.prefixlen != 32:
                print "Can't handle prefixes < 32"
            ip  = str(value)[:-3]
        elif isinstance(v, IPAddr):
            ip = str(v)
        else:
            print "Can't handle non-IP field"

        if k == "srcip":
            col["srcip"] = "'{0}'".format(ip)
        elif k == "dstip":
            col["dstip"] = "'{0}'".format(ip)
        else:
            print "Unknown rewrite field", k

    rule = "CREATE OR REPLACE RULE {0}_repair AS\n  ON DELETE TO {0}_violation\n"
    rule += "  DO INSTEAD (\n"
    rule += "\n".join("   UPDATE rm_match SET {0} = {1};".format(k,v) for k,v in col.iteritems())
    rule += "\n);"
    return rule

class PyreticConsole(AppConsole):
    def __init__(self, db, env, components):
        AppConsole.__init__(self, db, env, components)
        updatePath()
        self.do_load("")

    def parse_match(self, idx, match):
        from ipaddr import IPv4Network
        cols = {}
        cols["id"] = idx

        for field, value in match.map.iteritems():
            if isinstance(value, IPv4Network):
                if value.prefixlen != 32:
                    print "Can't handle prefixes < 32"
                    break

                ip  = str(value)[:-3]
                cols[field] = ip

        return cols

    def parse_if(self, idx, pol):
        from pyretic.core.language import match, if_, IdentityClass, modify
        from pyretic.core.network import IPAddr

        # if isinstance(pol.pred, match):
        #     raise Exception("Can't support non-match if predicates yet")

        if not isinstance(pol.t_branch, modify):
            raise Exception("Can't support non-modify if's yet")

        if not isinstance(pol.f_branch, IdentityClass):
            raise Exception("Can't support if-false branches yet")

        col = self.parse_match(idx, pol.pred)

        for k,v in pol.t_branch.map.iteritems():
            ip = None
            if isinstance(v, IPv4Network):
                if v.prefixlen != 32:
                    print "Can't handle prefixes < 32"
                    break

                ip  = str(value)[:-3]
            elif isinstance(v, IPAddr):
                ip = str(v)
            else:
                print "Can't handle non-IP field"

            if k == "srcip":
                col["rewrite"] = True
                col["rewrite_srcip"] = ip
            elif k == "dstip":
                col["rewrite"] = True
                col["rewrite_dstip"] = ip
            else:
                print "Unknown rewrite field", k

        return col

    def extract_matches(self, policy):
        from pyretic.core.language import CombinatorPolicy, union, sequential, \
            negate, match, if_, drop

        matches = []
        subpols = policy.policies
        drop_action = False

        cols = []
        for pol in subpols:
            if pol == drop:
                drop_action = True

            if isinstance(pol, if_):
                col = self.parse_if(len(cols), pol)
                cols.append(col)

            elif isinstance(pol, union):
                subpols.extend(pol.policies)
            elif isinstance(pol, sequential):
                subpols.extend(pol.policies)
            elif isinstance(pol, negate):
                subpols.extend(pol.policies)
            elif isinstance(pol, match):
                matches.append(pol)

        for match in matches:
            col = self.parse_match(len(cols), match)
            if drop_action:
                col['drop'] = "true"
            cols.append(col)

        return cols

    def do_load(self, line):
        # app = "pyfw"
        #app = "rewrite"
        app = "nat"

        try:
            appmod = importlib.import_module(app)
            policy = appmod.main()
            # print policy

            policy_tree = parse_policy(policy, app)
            print policy_tree
            # sql = convert_tree(policy_tree)
            # print sql

            # print policy

            # fname = "pyretic_{0}.sql".format(app)
            # open(fname, 'w').close()
            # with open(fname, 'w') as f:
            #     f.write(sql.format(app))


            # self.db.cursor.execute(sql.format(app))
            # cols = self.extract_matches(policy)
            # # print cols

            # insert = "insert into pyr_{0}_policy (%s) values %s".format(app)
            # for col in cols:
            #     columns = ','.join(col.keys())
            #     values = tuple(col.values())
            #     self.db.cursor.execute(self.db.cursor.mogrify(insert, ([AsIs(columns)] + [values])))

            # #self.db.cursor.execute("SELECT * FROM pyr_pyfw_policy;")
            # #print self.db.cursor.fetchall()

            # # cleanup
            # self.db.cursor.execute(cleanup.format(app))
        except Exception as e:
            print "Could not load app {0}: {1}".format(app, e)

    def do_echo(self, line):
        "Test command, echo arguments"
        print self.__class__.__name__, "says:", line

shortcut = "py"
description = "Pyretic runtime console"
console = PyreticConsole
