from datetime import datetime
from zoneinfo import ZoneInfo # Python 3.9 and above

# Convert UTC Time
def UTCToLocalTimezone(utc: datetime, timezone: str) -> datetime:
    """
    Convert UTC TimeStamp to Local Time for Given Timezone
    """
    # utc datetime is not timezone aware, manually replace tzinfo
    return utc.replace(tzinfo=ZoneInfo('UTC')).astimezone(ZoneInfo(timezone))
