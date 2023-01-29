from aws_utils import fetch_instances, fetch_snapshots_per_volume


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
