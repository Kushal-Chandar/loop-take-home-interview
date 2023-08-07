from datetime import datetime, timedelta, time
from psycopg2 import Binary
from typing import List, Tuple
from timezone_conversion import UTCToLocalTimezone, ChangeTimezone
from postgres import PostgresDatabase
import json

db = PostgresDatabase()


# hard coding max timestamp to current timestamp
def getCurrentTimeStamp() -> datetime | None:
    db.runQuery(
        f"""SELECT MAX(timestamp_utc::timestamp without time zone) FROM store_status"""
    )
    current = db.fetchOne()
    return current[0] if current else None


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
    db.runQuery(
        f"""
    SELECT timestamp_utc, status
    FROM store_status
    WHERE timestamp_utc >= '{CURRENT_TIMESTAMP}'::timestamp - INTERVAL '{interval}' AND store_id = {store_id}
    ORDER BY timestamp_utc DESC
    """
    )
    return db.fetchAll()


def getTimestampsLastWeek(store_id: int) -> List[Tuple[datetime, str]]:
    return getTimestampsStatusInterval(store_id=store_id, interval="1 week")


def getTimestamps(store_id: int) -> List[Tuple[datetime, str]]:
    db.runQuery(
        f"""
    SELECT timestamp_utc, status
    FROM store_status
    WHERE timestamp_utc >= AND store_id = {store_id}
    ORDER BY timestamp_utc DESC
    """
    )
    return db.fetchAll()


def getStoreTimezone(store_id: int) -> str:
    db.runQuery(
        f"""SELECT timezone_str FROM store_timezones WHERE store_id = {store_id}"""
    )
    timezone = db.fetchOne()
    if timezone == None or (not timezone[0]):
        return "America/Chicago"
    else:
        return timezone[0]


def is247Operational(store_id: int) -> bool:
    db.runQuery(
        f"""
    SELECT start_time_local, end_time_local
    FROM business_hours
    WHERE store_id = {store_id}
    """
    )
    business_hours = db.fetchAll()
    return True if len(business_hours) == 0 else False


def getBusinessHourTimestamp(
    store_id: int, timestamp: datetime, timezone: str
) -> Tuple[datetime, datetime] | None:
    """
    Return business hour for particular timestamp or None, in local time.
    """
    db.runQuery(
        f"""
    SELECT start_time_local, end_time_local
    FROM business_hours
    WHERE store_id = {store_id} AND "dayOfWeek" = {timestamp.weekday()}
    """
    )
    business_hours: List[Tuple[time, time]] = db.fetchAll()
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
def ProcessRequest(report_id: int):
    db.runQuery("""SELECT DISTINCT store_id FROM store_status ORDER BY store_id ASC""")
    stores = db.fetchAll()

    data = []

    db.runQuery(
        """
    CREATE TABLE IF NOT EXISTS reports (
        report_id SERIAL PRIMARY KEY,
        report TEXT
    )
    """
    )

    for store in stores:
        store_id: int = store[0]
        timezone: str = getStoreTimezone(store_id=store_id)
        data_store = {
            "store_id": store_id,
            "uptime_last_hour(in minutes)": 0,
            "uptime_last_day(in hours)": 0,
            "uptime_last_week(in hours)": 0,
            "downtime_last_hour(in minutes)": 0,
            "downtime_last_day(in hours)": 0,
            "downtime_last_week(in hours)": 0,
        }
        print(store_id)
        uptime_hour_min, uptime_day_min, uptime_week_min = 0, 0, 0
        downtime_hour_min, downtime_day_min, downtime_week_min = 0, 0, 0
        store_status_timestamps: List[Tuple[datetime, str]] = getTimestampsLastWeek(
            store_id
        )

        if len(store_status_timestamps) == 0:
            continue

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

                    if (
                        len(store_businesshour_timestamps) > 0
                        and store_businesshour_timestamps[-1][-1][0]
                        == current_local_business_hours_start
                        and store_businesshour_timestamps[-1][0][0]
                        == current_local_business_hours_end
                    ):
                        store_businesshour_timestamps[-1].insert(
                            -1, (local_timestamp, active)
                        )
                        # updating business hours end timestamp with last but one timestamp
                        business_end = store_businesshour_timestamps[-1].pop()

                        store_businesshour_timestamps[-1].append(
                            (business_end[0], active)
                        )
                    else:
                        store_businesshour_timestamps.append(
                            [
                                (current_local_business_hours_end, False),
                                (local_timestamp, active),
                                (current_local_business_hours_start, active),
                            ]
                        )
            if len(store_businesshour_timestamps) >= 1:
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

        one_hour_before = present_timestamp_local - timedelta(hours=1)
        one_day_before = present_timestamp_local - timedelta(days=1)

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
                timediff = ((last_timestamp - local_timestamp).total_seconds()) / 60

                if track_hour:
                    if local_timestamp >= one_hour_before:
                        uptime_hour_min += timediff if active else 0
                        downtime_hour_min += timediff if not active else 0

                    else:
                        remaining_time = max(
                            0, ((last_timestamp - one_hour_before).total_seconds()) / 60
                        )
                        uptime_hour_min += remaining_time if active else 0
                        downtime_hour_min += remaining_time if not active else 0
                        track_hour = False

                if track_day:
                    if local_timestamp >= one_day_before:
                        uptime_day_min += timediff if active else 0
                        downtime_day_min += timediff if not active else 0
                    else:
                        remaining_time = max(
                            0, ((last_timestamp - one_day_before).total_seconds()) / 60
                        )
                        uptime_day_min += remaining_time if active else 0
                        downtime_day_min += remaining_time if not active else 0
                        track_day = False

                uptime_week_min += timediff if active else 0
                downtime_week_min += timediff if not active else 0

                last_timestamp = local_timestamp

        data_store["store_id"] = store_id
        data_store["uptime_last_hour(in minutes)"] = round(uptime_hour_min)
        data_store["uptime_last_day(in hours)"] = round(uptime_day_min)
        data_store["uptime_last_week(in hours)"] = round(uptime_week_min)
        data_store["downtime_last_hour(in minutes)"] = round(downtime_hour_min)
        data_store["downtime_last_day(in hours)"] = round(downtime_day_min)
        data_store["downtime_last_week(in hours)"] = round(downtime_week_min)

        data.append(data_store)
        break

    db.runQuery(
        f"INSERT INTO reports (report_id, report) VALUES ({report_id}, '{(json.dumps(data))}')"
    )
