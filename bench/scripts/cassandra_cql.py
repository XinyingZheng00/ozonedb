#!/usr/bin/env python3
"""Minimal CQL client for cassandra_ctl.sh, run from the venv that setup.sh
builds ($CASSANDRA_INSTALL_DIR/pyenv).

Why this exists: the cqlsh bundled with Cassandra 5.0 refuses Python >= 3.12
(its driver imports asyncore, removed in 3.12), and the CloudLab image ships
only Python 3.12. So the bench installs cassandra-driver in a venv and drives
CQL through this instead of cqlsh. The libev reactor (built from libev-dev) is
the one that actually connects on 3.12; the asyncio reactor times out.

Usage:
    cassandra_cql.py --host H --port P -e "CQL; CQL; ..."
    cassandra_cql.py --host H --port P -f statements.cql
    cassandra_cql.py --host H --port P --ping        # exit 0 when CQL answers

Statements are split on ';'. Rows from the last SELECT are printed one per line
(comma-joined columns). Exit non-zero on any failure.
"""
import argparse
import sys
import warnings

# The driver's legacy-parameter DeprecationWarning is noise on every call.
warnings.filterwarnings("ignore", category=DeprecationWarning)


def _connect(host, port, timeout):
    from cassandra.cluster import Cluster
    from cassandra.io.libevreactor import LibevConnection
    from cassandra.policies import WhiteListRoundRobinPolicy
    cluster = Cluster(
        [host], port=port, connection_class=LibevConnection,
        connect_timeout=timeout, control_connection_timeout=timeout,
        load_balancing_policy=WhiteListRoundRobinPolicy([host]),
    )
    return cluster, cluster.connect()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=9042)
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("-e", "--execute", help="semicolon-separated statements")
    ap.add_argument("-f", "--file", help="file of statements")
    ap.add_argument("--ping", action="store_true", help="connect + trivial query, print OK")
    args = ap.parse_args()

    try:
        cluster, session = _connect(args.host, args.port, args.timeout)
    except Exception as e:
        sys.stderr.write(f"cassandra_cql: connect failed: {e}\n")
        return 2

    try:
        if args.ping:
            session.execute("SELECT release_version FROM system.local")
            print("OK")
            return 0
        text = args.execute if args.execute else (open(args.file).read() if args.file else sys.stdin.read())
        rows = None
        for stmt in [s.strip() for s in text.split(";") if s.strip()]:
            rows = session.execute(stmt)
        if rows is not None:
            for r in rows:
                print(", ".join(str(v) for v in r))
        return 0
    except Exception as e:
        sys.stderr.write(f"cassandra_cql: {e}\n")
        return 1
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    sys.exit(main())
