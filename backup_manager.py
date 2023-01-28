import boto3
from date_utils import *

# Logic for 1
ec2_client = boto3.client("ec2")


def fetch_instances():
    return [
        instance
        for group in ec2_client.describe_instances()["Reservations"]
        for instance in group["Instances"]
    ]


# create snapshot
def create_snapshot(volume):
    ec2_client.create_snapshot(VolumeId=volume)


# delete a snapshot
def delete_snapshot(snapshot):
    ec2_client.delete_snapshot(SnapshotId=snapshot)


def fetch_volumes():
    vms = parse_vms(fetch_instances())
    volumes = [vm["VolumeID"] for vm in vms]
    return volumes


def fetch_snapshots_per_volume(volume_id):
    return ec2_client.describe_snapshots(
        Filters=[{"Name": "volume-id", "Values": volume_id}]
    )


def parse_snapshots():
    # ['vol-06abb4d68e75ae30e', "vol-06abb4d68e75ae30e"]
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
    to_backup = get_volumes_to_backup(potential_machines_to_backup, all_backups)
    machines_to_snapshot = apply_retention_policy()
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
