import psycopg2, os
from dotenv import load_dotenv
from datetime import datetime, timedelta, time
from typing import List, Tuple
from timezone_conversion import UTCToLocalTimezone, ChangeTimezone

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
    return getTimestampsStatusInterval(store_id=store_id, interval="1 hour")


def getTimestampsLastDay(store_id: int) -> List[Tuple[datetime, str]]:
    return getTimestampsStatusInterval(store_id=store_id, interval="1 day")


def getTimestampsLastWeek(store_id: int) -> List[Tuple[datetime, str]]:
    return getTimestampsStatusInterval(store_id=store_id, interval="1 week")


def getTimestamps(store_id: int) -> List[Tuple[datetime, str]]:
    cur.execute(
        f"""
    SELECT timestamp_utc, status
    FROM store_status
    WHERE timestamp_utc >= AND store_id = {store_id}
    ORDER BY timestamp_utc DESC
    """
    )
    return cur.fetchall()


def getStoreTimezone(store_id: int) -> str:
    cur.execute(
        f"""SELECT timezone_str FROM store_timezones WHERE store_id = {store_id}"""
    )
    timezone = cur.fetchone()
    if timezone == None or (not timezone[0]):
        return "America/Chicago"
    else:
        return timezone[0]


def is247Operational(store_id: int) -> bool:
    cur.execute(
        f"""
    SELECT start_time_local, end_time_local
    FROM business_hours
    WHERE store_id = {store_id}
    """
    )
    business_hours = cur.fetchall()
    return True if len(business_hours) == 0 else False


def getBusinessHourTimestamp(
    store_id: int, timestamp: datetime, timezone: str
) -> Tuple[datetime, datetime] | None:
    """
    Return business hour for particular timestamp or None, in local time.
    """
    cur.execute(
        f"""
    SELECT start_time_local, end_time_local
    FROM business_hours
    WHERE store_id = {store_id} AND "dayOfWeek" = {timestamp.weekday()}
    """
    )
    business_hours: List[Tuple[time, time]] = cur.fetchall()
    for [start_time, end_time] in business_hours:
        if timestamp.time() >= start_time and timestamp.time() <= end_time:
            return (
                ChangeTimezone(
                    datetime.combine(timestamp.date(), start_time), timezone
                ),
                ChangeTimezone(datetime.combine(timestamp.date(), end_time), timezone),
            )
    return None


# Get all distinct stores
cur.execute("""SELECT DISTINCT store_id FROM store_status""")
stores = cur.fetchall()

for store in stores:
    # store_id: int = store[0]
    # store_id = 5125947543803222292
    # store_id = 85496058573776375
    # store_id = 7828565466095434540
    store_id = 2570905277901393
    timezone: str = getStoreTimezone(store_id=store_id)
    store_status_timestamps: List[Tuple[datetime, str]] = getTimestampsLastWeek(
        store_id
    )

    # last timestamp week assumption, store status same as last but one timestamp's store status
    store_status_timestamps.append(
        (CURRENT_TIMESTAMP - timedelta(weeks=1), store_status_timestamps[-1][1])
    )

    present_timestamp_local = UTCToLocalTimezone(CURRENT_TIMESTAMP, timezone)
    store_businesshour_timestamps: List[List[Tuple[datetime, bool]]] = []
    operational_247 = is247Operational(store_id)

    if not operational_247:
        # Assuming operational hours are unique and don't overlop
        present_timestamp_business_hours = getBusinessHourTimestamp(
            store_id, present_timestamp_local, timezone
        )
        if present_timestamp_business_hours:
            [
                present_timestamp_business_hours_start,
                present_timestamp_business_hours_end,
            ] = present_timestamp_business_hours
            store_businesshour_timestamps.append(
                [
                    # we are going in reverse order
                    (present_timestamp_business_hours_end, False),
                    (
                        present_timestamp_local,
                        False,
                    ),  # store can be assumed inactive currently
                    (present_timestamp_business_hours_start, False),
                ]
            )
        print(present_timestamp_business_hours)
        for [timestamp_utc, status] in store_status_timestamps:
            active: bool = True if status == "active" else False
            local_timestamp: datetime = UTCToLocalTimezone(timestamp_utc, timezone)
            current_local_business_hours = getBusinessHourTimestamp(
                store_id, local_timestamp, timezone
            )
            if current_local_business_hours:
                [
                    current_local_business_hours_start,
                    current_local_business_hours_end,
                ] = current_local_business_hours

                previous_timestamp_range_end = store_businesshour_timestamps[-1][0][0]
                previous_timestamp_range_start = store_businesshour_timestamps[-1][-1][
                    0
                ]
                if (
                    len(store_businesshour_timestamps) > 0
                    and previous_timestamp_range_start
                    == current_local_business_hours_start
                    and previous_timestamp_range_end == current_local_business_hours_end
                ):
                    store_businesshour_timestamps[-1].insert(
                        -1, (local_timestamp, active)
                    )
                    print("inserted: ", local_timestamp, active)
                    # updating business hours end timestamp with last but one timestamp
                    business_end = store_businesshour_timestamps[-1].pop()

                    store_businesshour_timestamps[-1].append((business_end[0], active))
                    print("changed: ", store_businesshour_timestamps[-1][-1])
                else:
                    store_businesshour_timestamps.append(
                        [
                            (current_local_business_hours_end, False),
                            (local_timestamp, active),
                            (current_local_business_hours_start, active),
                        ]
                    )
        store_businesshour_timestamps[
            -1
        ].pop()  # remove timestamp that crossed 7 days,  (current_local_business_hours_start, None)
        store_businesshour_timestamps[0].pop(
            0
        )  # remove timestamp after current timestamp ,  (present_local_business_hours_end, None)
    else:
        store_businesshour_timestamps.append([(present_timestamp_local, False)])
        for [timestamp_utc, status] in store_status_timestamps:
            active: bool = True if status == "active" else False
            local_timestamp: datetime = UTCToLocalTimezone(timestamp_utc, timezone)
            store_businesshour_timestamps[0].append((local_timestamp, active))

    for store_businesshour_timestamp in store_businesshour_timestamps:
        print("----------------------------------------------------------------------")
        for ind_timestamp in store_businesshour_timestamp:
            print(ind_timestamp)
        print("----------------------------------------------------------------------")
    print(store_id, timezone)

    # Get all timestamps of the store
    uptime_hour_min, uptime_day_min, uptime_week_min = 0, 0, 0
    downtime_hour_min, downtime_day_min, downtime_week_min = 0, 0, 0

    one_hour_before = present_timestamp_local - timedelta(hours=1)
    one_day_before = present_timestamp_local - timedelta(days=1)
    one_week_before = present_timestamp_local - timedelta(weeks=1)

    track_hour = True
    track_day = True
    for store_businesshour_timestamp in store_businesshour_timestamps:
        last_timestamp: datetime = store_businesshour_timestamp[0][0]
        # going to be current timestamp or business_hours_end timestamp

        for timestamp_idx, [local_timestamp, active] in enumerate(
            store_businesshour_timestamp
        ):
            if timestamp_idx == 0:
                # we have set this value as our last timestamp
                continue

            # Note: We judging the status of the store by last known status
            # active: bool = True if status == "active" else False
            # local_timestamp: datetime = UTCToLocalTimezone(timestamp_utc, timezone)
            timediff = ((last_timestamp - local_timestamp).total_seconds()) / 60
            # print(last_timestamp, local_timestamp, timediff, status)
            print(local_timestamp, last_timestamp, timediff)

            if track_hour:
                if local_timestamp >= one_hour_before:
                    uptime_hour_min += timediff if active else 0
                    downtime_hour_min += timediff if not active else 0

                else:
                    remaining_time = (
                        (last_timestamp - one_hour_before).total_seconds()
                    ) / 60
                    uptime_hour_min += remaining_time if active else 0
                    downtime_hour_min += remaining_time if not active else 0
                    track_hour = False

            if track_day:
                if local_timestamp >= one_day_before:
                    uptime_day_min += timediff if active else 0
                    downtime_day_min += timediff if not active else 0
                else:
                    remaining_time = (
                        (last_timestamp - one_day_before).total_seconds()
                    ) / 60
                    uptime_day_min += remaining_time if active else 0
                    downtime_day_min += remaining_time if not active else 0
                    track_day = False

            uptime_week_min += timediff if active else 0
            downtime_week_min += timediff if not active else 0

            last_timestamp = local_timestamp
        # break
    print(
        round(uptime_hour_min),
        round(uptime_day_min / 60),
        round(uptime_week_min / 60),
        round(downtime_hour_min),
        round(downtime_day_min / 60),
        round(downtime_week_min / 60),
        CURRENT_TIMESTAMP,
    )
    print(store_id, timezone)
    break

conn.commit()

cur.close()
conn.close()
