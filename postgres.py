import psycopg2, os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import List, Tuple
from timezone_conversion import UTCToLocalTimezone

load_dotenv()

HOST = os.getenv("HOST")
DBNAME = os.getenv("DBNAME")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
PORT = os.getenv("PORT")


conn = psycopg2.connect(
    host=HOST, dbname=DBNAME, user=USER, password=PASSWORD, port=PORT
)

cur = conn.cursor()


# hard coding max timestamp to current timestamp
def getCurrentTimeStamp() -> datetime | None:
    cur.execute(
        f"""SELECT MAX(timestamp_utc::timestamp without time zone) FROM store_status"""
    )
    current = cur.fetchone()
    if current:
        return current[0]
    else:
        return None


CURRENT_TIMESTAMP = getCurrentTimeStamp()
if not CURRENT_TIMESTAMP:
    exit()


def getTimestampsStatusInterval(
    store_id: int, interval: str
) -> List[Tuple[datetime, str]]:
    """
    Order Descending
    interval can be:
    1 hour
    1 day
    1 week
    """
    cur.execute(
        f"""
    SELECT timestamp_utc, status
    FROM store_status
    WHERE timestamp_utc >= '{CURRENT_TIMESTAMP}'::timestamp - INTERVAL '{interval}' AND store_id = {store_id}
    ORDER BY timestamp_utc DESC
    """
    )
    return cur.fetchall()


def getTimestampsLastHour(store_id: int) -> List[Tuple[datetime, str]]:
    return getTimestampsStatusInterval(store_id, "1 hour")


def getTimestampsLastDay(store_id: int) -> List[Tuple[datetime, str]]:
    return getTimestampsStatusInterval(store_id, "1 day")


def getTimestampsLastWeek(store_id: int) -> List[Tuple[datetime, str]]:
    return getTimestampsStatusInterval(store_id, "1 week")


def getBusinessHoursTimestampDay(
    store_id: int, timestamp: datetime
) -> Tuple[datetime, datetime] | None:
    """
    Returns the business hours for day on which the timestamp was recorded
    Else returns null
    """
    cur.execute(
        f"""
    SELECT start_time_local, end_time_local
    FROM business_hours
    WHERE store_id = {store_id} and "dayOfWeek" = {timestamp.weekday()}
    """
    )
    business_hours = cur.fetchall()
    # business_hours not given
    if len(business_hours) != 0:
        # business_hours.append((datetime.time(0,0), datetime.time(24,)))
        print(business_hours[0])

    for timeslot in business_hours:
        [start_time, end_time] = timeslot
        if timestamp.time() >= start_time and timestamp.time() <= end_time:
            return timeslot
    return None


cur.execute("""SELECT * FROM store_timezones""")
stores_with_timezones = cur.fetchall()

for store in stores_with_timezones:
    store_id: int = store[0]
    timezone: str = store[1] if store[1] else "America/Chicago"
    store_status_timestamps: List[Tuple[datetime, str]] = getTimestampsLastWeek(
        store_id
    )
    current_timestamp_local = UTCToLocalTimezone(CURRENT_TIMESTAMP, timezone)
    last_time_stamp = current_timestamp_local
    uptime_hour_min = 0
    uptime_day_min = 0
    uptime_week_min = 0
    downtime_hour_min = 0
    downtime_day_min = 0
    downtime_week_min = 0
    for [timestamp_utc, status] in store_status_timestamps:
        active: bool = True if status == "active" else False
        local_timestamp: datetime = UTCToLocalTimezone(timestamp_utc, timezone)
        business_hours = getBusinessHoursTimestampDay(store_id, local_timestamp)
        print(local_timestamp, business_hours)
        if business_hours != None:
            if last_time_stamp.date() != local_timestamp.date():
                print("Hello")
                continue
            print(last_time_stamp, local_timestamp)
            timediff = ((last_time_stamp - local_timestamp).total_seconds()) / 60
            if local_timestamp >= current_timestamp_local - timedelta(hours=1):
                if status:
                    uptime_hour_min = uptime_hour_min + timediff
                else:
                    downtime_hour_min = downtime_hour_min + timediff
            if local_timestamp >= current_timestamp_local - timedelta(days=1):
                if status:
                    uptime_day_min = uptime_day_min + timediff
                else:
                    downtime_day_min = downtime_day_min + timediff
            if local_timestamp >= current_timestamp_local - timedelta(weeks=1):
                if status:
                    uptime_week_min = uptime_week_min + timediff
                else:
                    downtime_week_min = downtime_week_min + timediff
            print(
                uptime_hour_min,
                uptime_day_min,
                uptime_week_min,
                downtime_hour_min,
                downtime_day_min,
                downtime_week_min,
            )
        last_time_stamp = local_timestamp

    print(store_id, timezone)

    break


conn.commit()

cur.close()
conn.close()
