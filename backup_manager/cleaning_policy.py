from math import trunc
from date_utils import is_more_than_one_week, is_less_than_one_week, how_long_ago
from list_backup_utils import melt_snapshots_and_vms


def make_distinction():
    all_backups = melt_snapshots_and_vms()
    all_backups_filtered = [
        snapshot for snapshot in all_backups if snapshot["Snapshot_date"] != "Never"
    ]
    # Split them in 2 categories.
    backups_older_than_7_days = [
        snapshot
        for snapshot in all_backups_filtered
        if (is_more_than_one_week(snapshot["Snapshot_date"]))
    ]
    backups_younger_than_7_days = [
        snapshot
        for snapshot in all_backups_filtered
        if (is_less_than_one_week(snapshot["Snapshot_date"]))
    ]

    return [backups_older_than_7_days, backups_younger_than_7_days]


# For all backups older than 7 days; keep only the most recent one.


def backups_older_than_7_days():
    backups = make_distinction()
    _backups = backups[1]
    new_backups = []

    # find out how old a certain backup is.
    for backup in _backups:
        backup["weeks"] = trunc(how_long_ago(backup["Snapshot_date"]).days / 7)
        new_backups.append(backup)

    # new_backups = [backup for backup in new_backups if backup["weeks"]>0]

    # create nested dict, where first we look at every unique week
    # followed by machines that have a backup in that week
    # and note the most recent back up per week to find a list of backups
    # to keep. Consequently we take the backup passed from
    # make distinction to this function and delete everything
    # which didn't make our keep list.

    unique_weeks = set([backup["weeks"] for backup in new_backups])
    snapshots_to_keep = {}
    for week in unique_weeks:
        backups_of_the_day = [
            backup for backup in new_backups if backup["weeks"] == week
        ]
        machines_of_this_week = set([backup["InstanceId"] for backup in new_backups])

    snapshots_to_keep["{week}".format(week=week)] = _helper_function(
        machines_of_this_week, backups_of_the_day
    )

    return snapshots_to_keep


# For backups older than a week, per week, keep the most recent one.
def _helper_function(machines_of_this_week, backups_of_the_week):
    some_list = []
    for machine in machines_of_this_week:
        m = {}
        weekly_backups_machine = [
            backup for backup in backups_of_the_week if backup["InstanceId"] == machine
        ]
        dates = []
        for backup in weekly_backups_machine:
            dates.append(backup["Snapshot_date"])
        dates.sort(reverse=True)
        date_to_keep = dates[0]
        backup_to_keep = [
            backup["Snapshot_Id"]
            for backup in weekly_backups_machine
            if backup["Snapshot_date"] == date_to_keep
        ]
        m["{machine}".format(machine=machine)] = backup_to_keep[0]
        some_list.append(m)


def helper_function(machines_of_today, backups_of_the_day):
    some_list = []
    for machine in machines_of_today:
        m = {}
        daily_backups_machine = [
            backup for backup in backups_of_the_day if backup["InstanceId"] == machine
        ]
        dates = []
        for backup in daily_backups_machine:
            dates.append(backup["Snapshot_date"])
        dates.sort(reverse=True)
        date_to_keep = dates[0]
        backup_to_keep = [
            backup["Snapshot_Id"]
            for backup in backups_of_the_day
            if backup["Snapshot_date"] == date_to_keep
        ]
        m["{machine}".format(machine=machine)] = backup_to_keep[0]
        some_list.append(m)

    # returns a list which contains dicts in the form of [{machine: [backup_to_keep_of_the_day_for_that_machine]}]
    return some_list


def backups_younger_than_7_days():
    backups = make_distinction()
    _backups = backups[1]
    new_backups = []

    # find out how old a certain backup is.
    for backup in _backups:
        backup["days"] = how_long_ago(backup["Snapshot_date"]).days
        new_backups.append(backup)

    # create nested dict, where first we look at every unique day
    # followed by machines that have a backup on that day
    # and note the most recent back up per date to find a list of backups
    # to keep. Consequently after we take the backup passed from
    # make distinction to this function and delete everything
    # which didn't make our keep list.

    unique_days = set([backup["days"] for backup in new_backups])
    snapshots_to_keep = {}
    for day in unique_days:
        backups_of_the_day = [backup for backup in new_backups if backup["days"] == day]
        machines_of_today = set([backup["InstanceId"] for backup in new_backups])
        snapshots_to_keep["{day}".format(day=day)] = helper_function(
            machines_of_today, backups_of_the_day
        )

    return snapshots_to_keep


def snapshots_to_delete(snapshots_to_keep):
    all_backups = melt_snapshots_and_vms()
    all_snapshot_ids = [backup["Snapshot_Id"] for backup in all_backups]

    # Add none for convenience to ignore vm which have no snaps
    keep_snap_list = ["None"]
    for i, j in snapshots_to_keep.items():
        if j != None:
            for machine in j:
                for instance, snapshot in machine.items():
                    keep_snap_list.append(snapshot)
    snaps_to_delete = [snap for snap in all_snapshot_ids if snap not in keep_snap_list]

    return snaps_to_delete
