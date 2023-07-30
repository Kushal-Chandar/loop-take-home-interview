from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9 and above

def ChangeTimezone(utc: datetime, timezone: str) -> datetime:
    """
    Replaces timezone of datetime with given timezone
    """
    # utc datetime is not timezone aware, manually replace tzinfo
    return utc.replace(tzinfo=ZoneInfo(timezone))


# Convert UTC Time
def UTCToLocalTimezone(utc: datetime, timezone: str) -> datetime:
    """
    Convert UTC TimeStamp to Local Time for Given Timezone
    """
    # utc datetime is not timezone aware, manually replace tzinfo
    return ChangeTimezone(utc, 'UTC').astimezone(ZoneInfo(timezone))
