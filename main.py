# main.py
# Run:  pip install nicegui requests python-dateutil
#       python main.py

from datetime import datetime, timedelta, timezone
from dateutil import tz
from typing import List, Dict, Tuple
import hashlib
import json
import os
import sqlite3
import requests
from nicegui import ui
import matplotlib.pyplot as plt
import pandas as pd


API_BASE = "https://api.goaa.aero/flights"
API_KEY = ""
API_VERSION = "150"
DB_PATH = "data.db"

# Orlando timezone
TZ_ORL = tz.gettz("America/New_York")

# ----------------------- SQLite -----------------------

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    return conn

def _init_db() -> None:
    with _db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS flights (
            uid TEXT PRIMARY KEY,                 -- sha1(flight|dep_airport|dep_date)
            api_id TEXT,
            iata_flight TEXT,
            base_airport TEXT,
            dep_airport TEXT,
            arr_airport TEXT,
            dep_date TEXT,                        -- YYYY-MM-DD used in uid (local to dep airport; MCO uses Orlando time)
            scheduled_ts INTEGER,                 -- UTC epoch seconds
            is_arrival INTEGER,                   -- 1 = arrival, 0 = departure
            status TEXT,
            canceled INTEGER,                     -- 1 if canceled
            terminal TEXT,
            gate TEXT,
            raw_json TEXT,
            first_seen_utc INTEGER,
            last_seen_utc INTEGER
        );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sched ON flights(scheduled_ts);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_canceled ON flights(canceled);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_depkey ON flights(iata_flight, dep_airport, dep_date);")

def _freeze_data_at_nov_17() -> None:
    """Delete all flights after November 17th, 2025 to freeze data at when FAA order was rescinded."""
    with _db() as conn:
        result = conn.execute("DELETE FROM flights WHERE dep_date > '2025-11-17';")
        deleted = result.rowcount
        if deleted > 0:
            print(f"Deleted {deleted} flights after November 17th, 2025")

# ----------------------- Helpers -----------------------

def _dep_local_date(f: Dict) -> str:
    """Local service date for UID de-dup (MCO defined in Orlando time)."""
    ts = int(f.get("scheduledTimestamp") or 0)
    dep_ap = f.get("departureAirport") or ""
    base = TZ_ORL if dep_ap == "MCO" else timezone.utc
    d = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(base).date()
    return d.isoformat()

def _flight_uid(f: Dict) -> str:
    flight = f.get("iataOperatingAirlineFlightNumber") or f.get("codeShareFlightNumber") or ""
    dep_ap = f.get("departureAirport") or ""
    dep_date = _dep_local_date(f)
    token = f"{flight}|{dep_ap}|{dep_date}"
    return hashlib.sha1(token.encode("utf-8")).hexdigest()

def _is_canceled(f: Dict) -> bool:
    return "cancel" in ((f.get("status") or "").lower())

def _valid_visible(f: Dict) -> bool:
    return bool(f.get("isVisible", True)) and not bool(f.get("isDeleted", False))

def _now_utc_epoch() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def _to_utc_epoch(dt_local: datetime) -> int:
    return int(dt_local.astimezone(timezone.utc).timestamp())

def _today_orl_midnight() -> datetime:
    now = datetime.now(TZ_ORL)
    return now.replace(hour=0, minute=0, second=0, microsecond=0)

def rolling_window() -> Tuple[datetime, datetime]:
    start = _today_orl_midnight()
    end = start + timedelta(days=3)  # half-open [start, end)
    return start, end

def carrier_cancel_counts() -> Dict[str, int]:
    with _db() as conn:
        cur = conn.execute("""
            SELECT substr(iata_flight, 1, 2) AS carrier, COUNT(*) AS c
            FROM flights
            WHERE canceled = 1 AND iata_flight IS NOT NULL AND iata_flight <> ''
            GROUP BY 1
            ORDER BY c DESC;
        """)
        print({row["carrier"]: row["c"] for row in cur.fetchall()})
        return {row["carrier"]: row["c"] for row in cur.fetchall()}

def airport_cancel_counts() -> Dict[str, Dict[str, int]]:
    """
    Count frequency of each non-MCO airport as a departure (dep_airport) and arrival (arr_airport),
    and sum for total cancellations. Only includes flights where canceled = 1.
    Returns:
        A dict of top 5 airport_code -> {'Departures': X, 'Arrivals': Y, 'Total': Z}
    """
    with _db() as conn:
        # Departures: non-MCO as dep_airport, where canceled
        cur = conn.execute("""
            SELECT dep_airport AS airport, COUNT(*) AS cnt FROM flights
            WHERE dep_airport IS NOT NULL AND dep_airport != '' AND dep_airport != 'MCO'
                AND canceled = 1
            GROUP BY dep_airport;
        """)
        dep_counts = {row["airport"]: row["cnt"] for row in cur.fetchall()}
        # Arrivals: non-MCO as arr_airport, where canceled
        cur = conn.execute("""
            SELECT arr_airport AS airport, COUNT(*) AS cnt FROM flights
            WHERE arr_airport IS NOT NULL AND arr_airport != '' AND arr_airport != 'MCO'
                AND canceled = 1
            GROUP BY arr_airport;
        """)
        arr_counts = {row["airport"]: row["cnt"] for row in cur.fetchall()}

    airport_set = set(dep_counts) | set(arr_counts)
    airport_stats_all = {}
    for airport in airport_set:
        dep = dep_counts.get(airport, 0)
        arr = arr_counts.get(airport, 0)
        airport_stats_all[airport] = {
            "Departures": dep,
            "Arrivals": arr,
            "Total": dep + arr
        }

    # Sort by Total descending and take top 5
    top_airports = sorted(
        airport_stats_all.items(),
        key=lambda item: item[1]["Total"],
        reverse=True
    )[:10]
    # Return as dict (airport code: stats)
    print({airport: stats for airport, stats in top_airports})
    return {airport: stats for airport, stats in top_airports}

def daily_cancellation_percentages() -> List[Tuple[str, float, int, int]]:
    """
    Returns daily cancellation percentages.
    Returns:
        List of tuples: (date, percentage, total_flights, cancelled_flights)
    """
    with _db() as conn:
        cur = conn.execute("""
            SELECT
                dep_date,
                COUNT(*) as total,
                SUM(canceled) as cancelled,
                CAST(SUM(canceled) AS FLOAT) / COUNT(*) * 100 as percent
            FROM flights
            WHERE dep_date IS NOT NULL AND dep_date != ''
            GROUP BY dep_date
            ORDER BY dep_date;
        """)
        print([(row["dep_date"], row["percent"], row["total"], row["cancelled"]) for row in cur.fetchall()])
        return [(row["dep_date"], row["percent"], row["total"], row["cancelled"]) for row in cur.fetchall()]

def daily_cancellation_percentages_by_type() -> Dict[str, Dict[str, float]]:
    """
    Returns daily cancellation percentages broken down by arrivals and departures.
    Returns:
        Dict of date -> {'overall': %, 'arrivals': %, 'departures': %}
    """
    with _db() as conn:
        # Overall percentages
        cur = conn.execute("""
            SELECT
                dep_date,
                CAST(SUM(canceled) AS FLOAT) / COUNT(*) * 100 as percent
            FROM flights
            WHERE dep_date IS NOT NULL AND dep_date != ''
            GROUP BY dep_date
            ORDER BY dep_date;
        """)
        overall = {row["dep_date"]: row["percent"] for row in cur.fetchall()}

        # Arrivals percentages
        cur = conn.execute("""
            SELECT
                dep_date,
                CAST(SUM(canceled) AS FLOAT) / COUNT(*) * 100 as percent
            FROM flights
            WHERE dep_date IS NOT NULL AND dep_date != '' AND is_arrival = 1
            GROUP BY dep_date
            HAVING COUNT(*) > 0
            ORDER BY dep_date;
        """)
        arrivals = {row["dep_date"]: row["percent"] for row in cur.fetchall()}

        # Departures percentages
        cur = conn.execute("""
            SELECT
                dep_date,
                CAST(SUM(canceled) AS FLOAT) / COUNT(*) * 100 as percent
            FROM flights
            WHERE dep_date IS NOT NULL AND dep_date != '' AND is_arrival = 0
            GROUP BY dep_date
            HAVING COUNT(*) > 0
            ORDER BY dep_date;
        """)
        departures = {row["dep_date"]: row["percent"] for row in cur.fetchall()}

    # Combine into single dict
    all_dates = set(overall.keys()) | set(arrivals.keys()) | set(departures.keys())
    result = {}
    for date in sorted(all_dates):
        result[date] = {
            'overall': overall.get(date, 0.0),
            'arrivals': arrivals.get(date, 0.0),
            'departures': departures.get(date, 0.0)
        }
    print(result)
    return result

# ----------------------- Upsert & Queries -----------------------

def upsert_flight(f: Dict) -> None:
    if not _valid_visible(f):
        return
    uid = _flight_uid(f)
    now = _now_utc_epoch()
    canceled_new = 1 if _is_canceled(f) else 0
    dep_date = _dep_local_date(f)

    # Heuristic for arrival vs departure based on airports
    dep_ap = f.get("departureAirport")
    arr_ap = f.get("arrivalAirport")
    is_arrival = 1 if arr_ap == "MCO" and dep_ap != "MCO" else 0

    payload = {
        "uid": uid,
        "api_id": f.get("id"),
        "iata_flight": f.get("iataOperatingAirlineFlightNumber") or f.get("codeShareFlightNumber"),
        "base_airport": f.get("baseAirport"),
        "dep_airport": dep_ap,
        "arr_airport": arr_ap,
        "dep_date": dep_date,
        "scheduled_ts": int(f.get("scheduledTimestamp") or 0),
        "is_arrival": is_arrival,
        "status": f.get("status"),
        "canceled": canceled_new,
        "terminal": f.get("terminal"),
        "gate": f.get("gate"),
        "raw_json": json.dumps(f, separators=(",", ":"), ensure_ascii=False),
        "now": now,
        "today": _today_orl_midnight().date().isoformat(),
    }

    # Preserve cancellations before today: once canceled=1 for dep_date < today, never flip back to 0.
    with _db() as conn:
        conn.execute("""
            INSERT INTO flights
            (uid, api_id, iata_flight, base_airport, dep_airport, arr_airport,
             dep_date, scheduled_ts, is_arrival, status, canceled, terminal, gate,
             raw_json, first_seen_utc, last_seen_utc)
            VALUES
            (:uid, :api_id, :iata_flight, :base_airport, :dep_airport, :arr_airport,
             :dep_date, :scheduled_ts, :is_arrival, :status, :canceled, :terminal, :gate,
             :raw_json, :now, :now)
            ON CONFLICT(uid) DO UPDATE SET
                api_id=excluded.api_id,
                iata_flight=excluded.iata_flight,
                base_airport=excluded.base_airport,
                dep_airport=excluded.dep_airport,
                arr_airport=excluded.arr_airport,
                dep_date=excluded.dep_date,
                scheduled_ts=excluded.scheduled_ts,
                is_arrival=excluded.is_arrival,
                status=excluded.status,
                canceled=CASE
                    WHEN flights.dep_date < :today AND flights.canceled = 1 THEN 1
                    ELSE excluded.canceled
                END,
                terminal=excluded.terminal,
                gate=excluded.gate,
                raw_json=excluded.raw_json,
                last_seen_utc=excluded.last_seen_utc;
        """, payload)

def count_all_cancellations() -> int:
    with _db() as conn:
        cur = conn.execute("SELECT COUNT(*) AS c FROM flights WHERE canceled = 1;")
        return int(cur.fetchone()["c"])

def get_cancellation_percentage() -> float:
    """
    Returns the percentage of cancelled flights out of all flights, as a float (0-100), rounded to max 2 decimals.
    """
    with _db() as conn:
        cur = conn.execute("SELECT COUNT(*) AS c FROM flights;")
        total = int(cur.fetchone()["c"])
    cancelled = count_all_cancellations()
    if total == 0:
        return 0.0
    return round((cancelled / total) * 100, 2)


def list_all_cancellations() -> List[sqlite3.Row]:
    with _db() as conn:
        cur = conn.execute("""
            SELECT iata_flight, dep_airport, arr_airport, status, terminal, gate, scheduled_ts, is_arrival
            FROM flights
            WHERE canceled = 1
            ORDER BY scheduled_ts DESC;
        """)
        return cur.fetchall()

# ----------------------- API fetch -----------------------

def _day_range_epochs(day_local: datetime) -> Tuple[int, int]:
    start_local = day_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    # API supports inclusive ranges; use ..end to cover the whole day
    return int(start_utc.timestamp()), int(end_utc.timestamp())

def _fetch_day(day_local: datetime) -> List[Dict]:
    start_epoch, end_epoch = _day_range_epochs(day_local)
    params = {"scheduledTimestamp": f"{start_epoch}..{end_epoch}"}
    headers = {
        "accept": "application/json",
        "Api-Key": API_KEY or "",
        "Api-Version": API_VERSION,
        "Origin": "https://flymco.com",
    }
    r = requests.get(API_BASE, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json()
    # Accommodate possible shapes
    return data.get("flights") or data.get("data", {}).get("flights", []) or []

def ingest_between(start_local: datetime, end_local: datetime) -> int:
    """Fetch and store all flights with scheduled times in [start_local, end_local)."""
    processed = 0
    d = start_local.replace(hour=0, minute=0, second=0, microsecond=0)
    last_day = (end_local - timedelta(seconds=1)).date()
    while d.date() <= last_day:
        for f in _fetch_day(d):
            upsert_flight(f)
            processed += 1
        d += timedelta(days=1)
    return processed

# ----------------------- UI -----------------------

@ui.page("/")
def main_page() -> None:
    ui.page_title("MCO Cancellations • Rolling 3-Day Ingest • All-time Count")

    with ui.row().classes("w-full justify-center mt-8 text-white"):
        header = ui.label().classes("text-3xl font-bold")
    with ui.row().classes("w-full justify-center mt-2"):
        sub = ui.label().classes("text-sm text-white")
    with ui.row().classes("w-full justify-center mt-1"):
        window_lab = ui.label().classes("text-sm text-white")
    
    with ui.row().classes("w-full justify-center"):
        ui.label("Data insights").classes("text-lg font-semibold mt-2 text-white")
    with ui.row().classes("w-full justify-center"):
        cancellation_pie = ui.pyplot(figsize=(5, 5)).classes("mt-4")
        airports_bar = ui.pyplot(figsize=(7, 4)).classes("ml-8 mt-4")
        daily_line = ui.pyplot(figsize=(9, 5)).classes("mt-4")

    def _draw_top_airports_bar():
        # Get the cancellation stats per airport
        airport_stats = airport_cancel_counts()  # Should return dict of airport: {'Departures': x, 'Arrivals': y, 'Total': z}
        
        if not airport_stats:
            with airports_bar:
                plt.clf()
                plt.title("No airport cancellation data available.")
                airports_bar.update()
            return

        # Sort by 'Total' descending and keep only a reasonable number, e.g., top 10
        airport_df = pd.DataFrame.from_dict(airport_stats, orient='index')
        airport_df = airport_df.sort_values('Total', ascending=False).head(10)
        
        with airports_bar:
            plt.clf()
            # Plot Departures and Arrivals as stacked bars
            airport_df[['Departures', 'Arrivals']].plot(
                kind='bar',
                stacked=True,
                figsize=(7, 4),
                color=['#1f77b4', '#ff7f0e'],
                ax=plt.gca()
            )
            plt.ylabel('Number of Cancellations')
            plt.xlabel('Airport')
            plt.title('Top 10 cancelled flights by airport')
            plt.legend(title='Type')
            plt.tight_layout()
            airports_bar.update()

    def _draw_pie():
        counts = carrier_cancel_counts()
        with cancellation_pie:
            plt.clf()
            if counts:
                labels = list(counts.keys())
                sizes = list(counts.values())
                plt.pie(sizes, labels=labels, autopct='%1.0f%%')
                plt.title("Cancellations by carrier")
        cancellation_pie.update()

    def _draw_daily_line():
        daily_data = daily_cancellation_percentages()

        with daily_line:
            plt.clf()
            if not daily_data:
                plt.title("No daily cancellation data available.")
            else:
                dates = [item[0] for item in daily_data]
                percentages = [item[1] for item in daily_data]

                plt.plot(dates, percentages, marker='o', linewidth=2, markersize=4, color='#d62728')

                # Add percentage labels at each data point
                for i, pct in enumerate(percentages):
                    plt.text(i, pct, f'{pct:.1f}%', ha='center', va='bottom', fontsize=8, fontweight='bold')

                # Add vertical line at Nov 13, 2025 if it exists in the data
                target_date = '2025-11-13'
                if target_date in dates:
                    x_pos = dates.index(target_date)
                    plt.axvline(x=x_pos, color='gray', linestyle='--', linewidth=2, alpha=0.5, label='Government shutdown ends')
                    plt.legend(loc='best')

                plt.ylabel('Cancellation Rate (%)')
                plt.xlabel('Date')
                plt.title('Percentage of Cancellations of Daily Scheduled Flights')
                plt.xticks(rotation=45, ha='right')
                plt.grid(True, alpha=0.3)
                plt.tight_layout()
            daily_line.update()


    with ui.column().classes("w-full items-center justify-center mt-4"):
        ui.label("Cancelled flights").classes("text-lg font-semibold mt-2 text-white")
        table = ui.table(
            columns=[
                {"name": "when", "label": "Scheduled", "field": "when"},
                {"name": "dir", "label": "Arrival/Departure", "field": "dir"},
                {"name": "flight", "label": "Flight", "field": "flight"},
                {"name": "route", "label": "Route", "field": "route"},
                {"name": "status", "label": "Status", "field": "status"},
            ],
            rows=[],
            row_key="rowid",
        ).classes("mt-2 w-11/12")

    def _fmt_rows(rows: List[sqlite3.Row]) -> List[Dict]:
        out = []
        for idx, r in enumerate(rows):
            when_local = datetime.fromtimestamp(int(r["scheduled_ts"]), tz=timezone.utc).astimezone(TZ_ORL)
            dep = r["dep_airport"] or "?"
            arr = r["arr_airport"] or "?"
            direction = "Arrival" if int(r["is_arrival"]) == 1 or arr == "MCO" else "Departure"
            out.append({
                "rowid": f"{idx}",
                "when": when_local.strftime("%Y-%m-%d %H:%M"),
                "dir": direction,
                "flight": r["iata_flight"] or "—",
                "route": f"{dep} → {arr}",
                "status": r["status"] or "—",
            })
        return out

    def refresh_view():
        try:
            total = count_all_cancellations()
            percentcancelled = get_cancellation_percentage()
            header.set_text(f"{total:,} cancellations at MCO, thats {percentcancelled}% of total flights")

            start_local, end_local = rolling_window()
            window_lab.set_text(
                f"This data is frozen in time and monitors flights up to November 14th"
            )
            now_local = datetime.now(TZ_ORL).strftime("%Y-%m-%d %H:%M %Z")
            sub.set_text(f"Last updated {now_local}")

            rows = list_all_cancellations()
            table.rows = _fmt_rows(rows)
            table.update()
            _draw_pie()
            _draw_top_airports_bar()
            _draw_daily_line()
        except Exception as e:
            header.set_text("Error")
            sub.set_text(str(e))

    def tick():
        if not API_KEY:
            header.set_text("Missing MCO_API_KEY")
            sub.set_text("export MCO_API_KEY='your_api_key_here' and restart")
            return
        # Rolling 3-day ingest
        #start_local, end_local = rolling_window()
        #ingest_between(start_local, end_local)
        # Update UI from DB snapshot
        refresh_view()

    # Initial paint
    refresh_view()
    # Every 3 minutes
    #ui.timer(180, tick)

if __name__ in {"__main__", "__mp_main__"}:
    _init_db()
    _freeze_data_at_nov_17()  # Remove any data after November 14th
    ui.run(title="MCO Cancellations Dashboard", port=5001)
