from math import trunc
from date_utils import *
from aws_utils import *

# TODO: add asyncio


def fetch_volumes():
    vms = parse_vms(fetch_instances())
    volumes = [vm["VolumeID"] for vm in vms]
    return volumes


def parse_snapshots():
    volumes = fetch_volumes()
    snapshots = fetch_snapshots_per_volume(volumes)["Snapshots"]
    snapshot_dates = [
        {
            "Snapshot_Id": snapshot["SnapshotId"],
            "Snapshot_date": snapshot["StartTime"],
            "vol_id": snapshot["VolumeId"],
        }
        for snapshot in snapshots
    ]
    return snapshot_dates


def melt_snapshots_and_vms():
    vms = parse_vms(fetch_instances())
    all_vms_ids = [vm["InstanceId"] for vm in vms]
    snapshots = parse_snapshots()
    melted_snaps_vms = []
    for vm in vms:
        for snapshot in snapshots:
            if vm["VolumeID"] == snapshot["vol_id"]:
                volume_snapshots = {
                    "InstanceId": vm["InstanceId"],
                    "Backup Enabled": vm["Backup Enabled"],
                    "Disk": vm["Disk"],
                    "VolumeID": vm["VolumeID"],
                    "Snapshot_date": snapshot["Snapshot_date"],
                    "Snapshot_Id": snapshot["Snapshot_Id"],
                }
                melted_snaps_vms.append(volume_snapshots)
    vms_with_backups = [vm["InstanceId"] for vm in melted_snaps_vms]
    vms_with_no_backups = [
        x
        for x in all_vms_ids
        if not x in vms_with_backups or vms_with_backups.remove(x)
    ]

    for id in vms_with_no_backups:
        for vm in vms:
            if vm["InstanceId"] == id:
                vm["Snapshot_date"] = "Never"
                vm["Snapshot_Id"] = "None"
                melted_snaps_vms.append(vm)
    return melted_snaps_vms


def parse_vms(instances):
    information_holder = []
    for vm in instances:
        blockDeviceMappings = vm["BlockDeviceMappings"]
        vol = blockDeviceMappings[0]["Ebs"]["VolumeId"]
        tags = vm["Tags"]
        # Logic needed because dicts can be in reverse order.
        if tags[0]["Value"] == "false" or tags[0]["Value"] == "true":
            _dict = {
                "InstanceId": vm["InstanceId"],
                "Backup Enabled": tags[0]["Value"],
                "Disk": tags[1]["Value"],
                "VolumeID": vol,
            }
        else:
            _dict = {
                "InstanceId": vm["InstanceId"],
                "Backup Enabled": tags[1]["Value"],
                "Disk": tags[0]["Value"],
                "VolumeID": vol,
            }
        information_holder.append(_dict)
    return information_holder


# Logic for 2
def create_backup(volumes_to_snapshot):
    for volume in volumes_to_snapshot:
        create_snapshot(volume)


def apply_retention_policy():
    potential_machines_to_backup = find_all_machines_with_backup_set_to_true()
    all_backups = melt_snapshots_and_vms()
    machines_to_snapshot = get_volumes_to_backup(
        potential_machines_to_backup, all_backups
    )
    volumes_to_snapshot = [machine["VolumeId"] for machine in machines_to_snapshot]
    return volumes_to_snapshot


def find_all_machines_with_backup_set_to_true():
    vms = parse_vms(fetch_instances())
    vms_of_interest = []
    for parsed_vm in vms:
        if parsed_vm["Backup Enabled"] == "true":
            vms_of_interest.append(
                {
                    "InstanceId": parsed_vm["InstanceId"],
                    "VolumeId": parsed_vm["VolumeID"],
                }
            )
    return vms_of_interest


def get_volumes_to_backup(potential_machines_to_backup, all_backups):
    # First finds all backups per machine
    # Then gets last and adds it to machine id
    # TODO: check if the machine which has backup enabled but has no backup yet is being covered
    potential_machines = [
        machine["InstanceId"] for machine in potential_machines_to_backup
    ]
    backups_per_machine = [
        machine
        for machine in all_backups
        if machine["InstanceId"] in potential_machines
    ]
    machine_dates = []
    for machine_id in potential_machines:
        backup_dates_per_machine = []
        for backup in backups_per_machine:
            if backup["InstanceId"] == machine_id:
                backup_dates_per_machine.append(backup["Snapshot_date"])
        backup_dates_per_machine.sort(reverse=True)
        machine_dates_holder = {
            "InstanceId": machine_id,
            "Last_Backup": backup_dates_per_machine[0],
            "VolumeId": backup["VolumeID"],
            "Prelim_Backup_Check": before_today(backup_dates_per_machine[0]),
        }
        machine_dates.append(machine_dates_holder)
    machine_dates = [
        machine for machine in machine_dates if machine["Prelim_Backup_Check"] == True
    ]
    return machine_dates


# Logic for 3

# First get all machines, then get all backups.


def apply_cleaning_policy():
    all_snaps_to_remove = []
    removal_7_days = snapshots_to_delete(backups_younger_than_7_days())
    removal_weeks = snapshots_to_delete(backups_older_than_7_days())
    all_snaps_to_remove.append(removal_7_days)
    all_snaps_to_remove.append(removal_weeks)
    final_cleaning_list = [item for sublist in all_snaps_to_remove for item in sublist]
    return final_cleaning_list


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
        for machine in j:
            for instance, snapshot in machine.items():
                keep_snap_list.append(snapshot)
    snaps_to_delete = [snap for snap in all_snapshot_ids if snap not in keep_snap_list]

    return snaps_to_delete


def clean_backups(snapshotids: list = None):
    for snapshotid in snapshotids:
        delete_snapshot(snapshotid)


# Answer 1

output_1 = melt_snapshots_and_vms()

# Answer 2

create_backup(volumes_to_snapshot=apply_retention_policy())
print("pause")
# Answer 3

clean_backups(snapshotids=apply_cleaning_policy())
