import os
import sys
import importlib

from psycopg2.extensions import AsIs

from ravel.util import Config
from ravel.app import AppConsole

# http://stackoverflow.com/questions/15178859/postgres-constraint-ensuring-one-column-of-many-is-present

# TODO:
# how to determine sid?
# handle nexthop view when port/nexthop doesn't exist
# enforce (max of a) single action
sql = """
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

CREATE OR REPLACE VIEW pyr_{0}_drop_violation AS (
    SELECT fid
    FROM rm
    WHERE src in (SELECT src FROM pyr_{0}_translation WHERE drop=true)
       OR dst in (SELECT dst FROM pyr_{0}_translation WHERE drop=true)
);

CREATE OR REPLACE VIEW pyr_{0}_nexthop AS (
    SELECT p.id,
           ports.nid
    FROM pyr_{0}_policy p
    LEFT JOIN ports
        ON p.sid = ports.sid AND
           p.fwd = ports.port
    WHERE ports.nid IS NOT NULL
);

CREATE OR REPLACE VIEW pyr_{0}_fwd_violation AS (
    SELECT DISTINCT fid
    FROM cf
    WHERE nid NOT IN (SELECT nid FROM pyr_{0}_nexthop
                      WHERE sid = sid)
);
"""

cleanup = """
DROP TABLE IF EXISTS pyr_{0}_policy CASCADE;
DROP TABLE IF EXISTS pyr_{0}_translation CASCADE;
DROP TABLE IF EXISTS pyr_{0}_nexthop CASCADE;
DROP VIEW IF EXISTS pyr_{0}_drop_violation CASCADE;
DROP VIEW IF EXISTS pyr_{0}_fwd_violation CASCADE;
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

    def extract_matches(self, policy):
        from pyretic.core.language import CombinatorPolicy, union, sequential, negate, match, drop
        matches = []
        subpols = policy.policies
        drop_action = False

        for pol in subpols:
            if pol == drop:
                drop_action = True

            if isinstance(pol, union):
                subpols.extend(pol.policies)
            elif isinstance(pol, sequential):
                subpols.extend(pol.policies)
            elif isinstance(pol, negate):
                subpols.extend(pol.policies)
            elif isinstance(pol, match):
                matches.append(pol)

        cols = []
        for idx, match in enumerate(matches):
            col = self.parse_match(idx, match)
            if drop_action:
                col['drop'] = "true"
            cols.append(col)

        return cols

    def do_load(self, line):
        app = "pyfw"

        try:
            appmod = importlib.import_module(app)
            policy = appmod.main()
            #print policy

            self.db.cursor.execute(sql.format(app))
            cols = self.extract_matches(policy)
            # print cols

            insert = "insert into pyr_{0}_policy (%s) values %s".format(app)
            for col in cols:
                columns = ','.join(col.keys())
                values = tuple(col.values())
                self.db.cursor.execute(self.db.cursor.mogrify(insert, ([AsIs(columns)] + [values])))

            #self.db.cursor.execute("SELECT * FROM pyr_pyfw_policy;")
            #print self.db.cursor.fetchall()

            # cleanup
            self.db.cursor.execute(cleanup.format(app))
        except Exception as e:
            print "Could not load app {0}: {1}".format(app, e)

    def do_echo(self, line):
        "Test command, echo arguments"
        print self.__class__.__name__, "says:", line

shortcut = "py"
description = "Pyretic runtime console"
console = PyreticConsole
