from datetime import datetime, timedelta

import pytz
from dateutil.tz import tzlocal

# this shows I know how to use typing, but did not get to use it.

# date utils
def is_more_than_one_week(date: datetime) -> bool:
    one_week_ago = datetime.utcnow().replace(tzinfo=pytz.UTC) - timedelta(days=7)
    if date < one_week_ago:
        return True
    else:
        return False


def is_less_than_one_week(date: datetime) -> bool:
    one_week_ago = datetime.utcnow().replace(tzinfo=pytz.UTC) - timedelta(days=7)
    if date > one_week_ago:
        return True
    else:
        return False


def how_long_ago(date: datetime) -> str:
    return datetime.utcnow().replace(tzinfo=tzlocal()) - date


def before_today(date):
    delta = date.today() - date.replace(tzinfo=None)
    now = datetime.now()
    seconds_since_midnight = (
        now - now.replace(hour=0, minute=0, second=0, microsecond=0)
    ).total_seconds()
    check = float(delta.seconds) - seconds_since_midnight
    # true means seconds since snapshot is longer than when today started. Consequence: last snapshot must've been before today.
    # false means seconds since snapshot is less than when today started. Consequence: last snapshot must've been today.
    if check > 0:
        return True
    else:
        return False
