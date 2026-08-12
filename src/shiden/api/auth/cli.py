"""Key management CLI.

    python -m shiden.api.auth.cli issue --name "Acme Energy" --markets 10YRO-TEL------P
    python -m shiden.api.auth.cli list
    python -m shiden.api.auth.cli revoke shiden_live_a1b2c3d4
    python -m shiden.api.auth.cli usage --days 7

Kept in argparse rather than adding a CLI framework: this is four
commands with no interactivity, and every dependency in an API business
is a dependency you have to keep patched.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone

from shiden.api.auth.store import ALL_MARKETS, KeyStore
from shiden.config.markets import MARKETS
from shiden.config.settings import settings


def _store(args: argparse.Namespace) -> KeyStore:
    return KeyStore(args.db or settings.auth_db_path)


def cmd_issue(args: argparse.Namespace) -> int:
    markets = tuple(args.markets) if args.markets else (ALL_MARKETS,)

    unknown = [m for m in markets if m != ALL_MARKETS and m not in MARKETS]
    if unknown:
        print(f"error: unknown market(s): {unknown}", file=sys.stderr)
        print(f"known markets: {sorted(MARKETS)}", file=sys.stderr)
        return 2

    secret, record = _store(args).issue_key(
        name=args.name,
        markets=markets,
        rate_limit_per_min=args.rate_limit,
        environment=args.environment,
    )

    print(f"Issued key for {record.name!r}")
    print()
    print(f"  {secret}")
    print()
    print("This is the only time the key is shown. Store it now.")
    print(f"  id          {record.id}")
    print(f"  markets     {', '.join(record.markets)}")
    print(f"  rate limit  {record.rate_limit_per_min}/min")
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    records = _store(args).list_keys(include_revoked=args.all)
    if not records:
        print("No keys issued yet.")
        return 0

    header = (
        f"{'ID':>4}  {'DISPLAY':<24} {'NAME':<24} "
        f"{'RATE':>6}  {'STATUS':<8} LAST USED"
    )
    print(header)
    print("-" * len(header))
    for r in records:
        status = "active" if r.is_active else "revoked"
        print(
            f"{r.id:>4}  {r.display:<24} {r.name[:24]:<24} "
            f"{r.rate_limit_per_min:>6}  {status:<8} {r.last_used_at or 'never'}"
        )
    return 0


def cmd_revoke(args: argparse.Namespace) -> int:
    if _store(args).revoke(args.key):
        print(f"Revoked {args.key}")
        return 0
    print(f"No active key matching {args.key!r}", file=sys.stderr)
    return 1


def cmd_usage(args: argparse.Namespace) -> int:
    since = None
    if args.days:
        since = (
            datetime.now(timezone.utc) - timedelta(days=args.days)
        ).isoformat(timespec="seconds")

    rows = _store(args).usage_summary(since=since)
    if not rows:
        print("No usage recorded.")
        return 0

    header = (
        f"{'KEY':<24} {'NAME':<24} {'REQS':>8} {'ERRORS':>7} "
        f"{'AVG MS':>8}  LAST SEEN"
    )
    print(header)
    print("-" * len(header))
    for row in rows:
        print(
            f"{row['key_display']:<24} {row['name'][:24]:<24} "
            f"{row['requests']:>8} {row['errors']:>7} "
            f"{row['avg_ms']:>8}  {row['last_seen']}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="shiden-keys", description="Manage Shiden API keys and inspect usage."
    )
    parser.add_argument("--db", help="override the key store path")
    sub = parser.add_subparsers(dest="command", required=True)

    issue = sub.add_parser("issue", help="mint a new API key")
    issue.add_argument("--name", required=True, help="client name, e.g. 'Acme Energy'")
    issue.add_argument(
        "--markets",
        nargs="*",
        help=f"entitled market ids, or omit for all ({ALL_MARKETS})",
    )
    issue.add_argument("--rate-limit", type=int, default=60, help="requests per minute")
    issue.add_argument(
        "--environment", choices=("live", "test"), default="live"
    )
    issue.set_defaults(func=cmd_issue)

    listing = sub.add_parser("list", help="list issued keys")
    listing.add_argument(
        "--all", action="store_true", help="include revoked keys"
    )
    listing.set_defaults(func=cmd_list)

    revoke = sub.add_parser("revoke", help="revoke a key by id or display prefix")
    revoke.add_argument("key")
    revoke.set_defaults(func=cmd_revoke)

    usage = sub.add_parser("usage", help="per-key usage summary")
    usage.add_argument("--days", type=int, default=30, help="lookback window")
    usage.set_defaults(func=cmd_usage)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
