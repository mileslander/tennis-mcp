import os
import sqlite3
import sys
import threading
import urllib.error
import urllib.request
from contextlib import closing
from datetime import datetime, timezone
from email.utils import formatdate
from pathlib import Path

from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

DB_URL = "https://github.com/mileslander/tennis-data-pipeline/releases/download/db-latest/tennis_data.db"

mcp = MCPServer("Tennis MCP")

db_path = Path(__file__).parent / "tennis_data.db"


class DbState:
    """What the background refresh is doing. Written by the refresh thread, read by tools."""

    def __init__(self):
        self.status = "checking"  # checking | downloading | up_to_date | failed
        self.error: str | None = None
        self.bytes_done = 0
        self.bytes_total: int | None = None

    def progress(self) -> str:
        mb = self.bytes_done / 1e6
        if self.bytes_total:
            return f"{mb:.0f}/{self.bytes_total / 1e6:.0f} MB"
        return f"{mb:.0f} MB so far"


state = DbState()


def _log(msg: str):
    print(f"[tennis-mcp] {msg}", file=sys.stderr)  # stdout is the MCP protocol channel


def _refresh_db():
    tmp = db_path.with_suffix(".db.tmp")
    try:
        req = urllib.request.Request(DB_URL)
        if db_path.exists():
            # Only send the file if it changed since our copy was written.
            req.add_header("If-Modified-Since", formatdate(db_path.stat().st_mtime, usegmt=True))
        with urllib.request.urlopen(req, timeout=60) as resp, open(tmp, "wb") as f:
            state.bytes_total = int(resp.headers.get("Content-Length") or 0) or None
            state.status = "downloading"
            while chunk := resp.read(1 << 20):
                f.write(chunk)
                state.bytes_done += len(chunk)
        os.replace(tmp, db_path)  # atomic swap
        state.status = "up_to_date"
        _log("database updated")
    except urllib.error.HTTPError as e:
        if e.code == 304:
            state.status = "up_to_date"
            _log("database already up to date")
        else:
            state.status, state.error = "failed", f"HTTP {e.code}"
            _log(f"database refresh failed: {e}")
    except Exception as e:
        state.status, state.error = "failed", str(e)
        _log(f"database refresh failed: {e}")
    finally:
        tmp.unlink(missing_ok=True)


def _connect():
    if not db_path.exists():
        if state.status == "failed":
            raise ToolError(
                f"Could not download the tennis database ({state.error}). "
                "Check the internet connection, then restart the server with /mcp."
            )
        raise ToolError(
            f"The tennis database is downloading for the first time ({state.progress()}). "
            "Try again in a minute."
        )
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return closing(conn)


def _freshness(conn) -> dict:
    """How current the data is, plus a notice when it may be missing recent results.

    Matches have no dates, only their tournament's start date, so recency is reported as
    the latest tournament week. Tournaments that week may still be in progress. 
    Reminder that Grand Slams and some Masters Series events are two weeks long.
    """
    built_at, latest_week = conn.execute("SELECT built_at, last_tournament_date FROM Metadata").fetchone()
    built = datetime.fromisoformat(built_at)
    age_days = (datetime.now(timezone.utc) - built).days
    info = {
        "latest_tournament_week": f"week of {latest_week} (tournaments in progress may have partial results)",
        "last_updated": built.strftime("%Y-%m-%d %H:%M UTC"),
    }
    copy = f"This copy was last updated {age_days} days ago (latest tournament week: {latest_week})"

    if state.status == "downloading":
        info["notice"] = (
            f"{copy}. A newer copy is downloading ({state.progress()}), so recent matches may be "
            "missing. Tell the user, and retry shortly for current results."
        )
    elif state.status == "checking":
        info["notice"] = (
            f"{copy}. Still checking for a newer database; retry shortly if recent results are needed."
        )
    elif state.status == "failed":
        info["notice"] = (
            f"Could not check for a newer database ({state.error}). {copy}, so recent matches "
            "may be missing. Tell the user."
        )
    return info

@mcp.tool()
def search_player(name: str) -> dict:
    """Look up a tennis player's player id by full name, e.g. 'Andy Roddick'.

    Returns every player with that name; use the pid with the other tools.
    """
    with _connect() as conn:
        rows = conn.execute(
            "SELECT pid, name FROM Players WHERE name = ? COLLATE NOCASE", (name,)
        ).fetchall()
        freshness = _freshness(conn)

    if not rows:
        raise ToolError(f"Player {name!r} not found")

    return {"players": [dict(r) for r in rows], **freshness}


@mcp.tool()
def get_player_profile(pid: str) -> dict:
    """Look up a tennis player's profile by player id (get a player's id from search_player)."""

    with _connect() as conn:
        row = conn.execute("SELECT * FROM Players WHERE pid = ?", (pid,)).fetchone()
        freshness = _freshness(conn)

    if row is None:
        raise ToolError(f"Player {pid!r} not found")

    return {"player": dict(row), **freshness}


@mcp.tool()
def head_to_head(p1_id: str, p2_id: str) -> dict:
    """Career head-to-head record between two players (ATP and WTA). 
    Pass each player's id separately. (get a player's id from search_player)"""

    wins_query = """SELECT (SELECT COUNT(*) FROM ATPMatches WHERE wid = :w AND lid = :l)
                         + (SELECT COUNT(*) FROM WTAMatches WHERE wid = :w AND lid = :l)"""
    name_query = "SELECT name FROM Players WHERE pid = ?"

    with _connect() as conn:
        names = {}
        for pid in (p1_id, p2_id):
            row = conn.execute(name_query, (pid,)).fetchone()
            if row is None:
                raise ToolError(f"Player {pid!r} not found")
            names[pid] = row["name"]
        p1_wins = conn.execute(wins_query, {"w": p1_id, "l": p2_id}).fetchone()[0]
        p2_wins = conn.execute(wins_query, {"w": p2_id, "l": p1_id}).fetchone()[0]
        freshness = _freshness(conn)

    p1_name, p2_name = names[p1_id], names[p2_id]

    return {
        "players": [
            {"pid": p1_id, "name": p1_name, "wins": p1_wins},
            {"pid": p2_id, "name": p2_name, "wins": p2_wins},
        ],
        "summary": f"{p1_name} {p1_wins}–{p2_wins} {p2_name}",
        **freshness,
    }


# Requires current ranking data: https://github.com/mileslander/tennis-data-pipeline/issues/3
# @mcp.tool()
def best_national_players(country: str, number_of_players: int = 5) -> dict:
    """Look up a country's best n players, default of 5 with 1 <= n <= 50. Uses standard three letter
    IOC abbreviations for countries, e.g. (GER, ARG). Returns each player's name, player id, ranking, 
    age, and birthplace (if available)"""

    if number_of_players < 1 or number_of_players > 50:
        raise ToolError("Invalid input: number of players must be in [1, 50]")

    query = """SELECT Players.name, Players.pid, Rankings.rank, Players.birthdate, Players.birthplace FROM Players
    INNER JOIN Rankings ON Players.pid = Rankings.pid WHERE nationality = ? AND Rankings.date 
    = (SELECT MAX(date) FROM Rankings) ORDER BY Rankings.rank LIMIT ?;"""

    with _connect() as conn:
        players_list = conn.execute(query, (country.upper(), number_of_players)).fetchall()

        freshness = _freshness(conn)

    if not players_list:
        raise ToolError(f"No ranked players found for {country!r}")

    return {
            "players": [dict(r) for r in players_list],
            **freshness
    }


# Requires current ranking data: https://github.com/mileslander/tennis-data-pipeline/issues/3
# @mcp.tool()
# def current_ranking(pid: str) -> str:
#   pass


# Requires historical ranking: https://github.com/mileslander/tennis-data-pipeline/issues/4
# @mcp.tool()
# def ranking_at_date(pid: str, date: str) -> str:
#   pass


if __name__ == "__main__":
    threading.Thread(target=_refresh_db, daemon=True).start()
    mcp.run()
