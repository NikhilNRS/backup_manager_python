from list_backup_utils import melt_snapshots_and_vms
from cleaning_policy import (
    backups_older_than_7_days,
    backups_younger_than_7_days,
    snapshots_to_delete,
)
from aws_utils import delete_snapshot, create_snapshot
from retainment_policy import (
    get_volumes_to_backup,
    find_all_machines_with_backup_set_to_true,
)

# TODO: add asyncio


class BackupManager:

    """A class for automating backups of virtual machines running on Amazon Web Services (AWS). It
    contains both functionality to make/delete snapshots of the Elastic Block Storage disks attached
    to AWS virtual machines, based on the labels of the virtual machines. In addition to this, it contains
    default retention - and cleaning policies to manage the backups/snapshots."""

    def __init__(self):
        self.list_basic_info = None

    def _set_bm_attribs_(self):
        self.list_basic_info = melt_snapshots_and_vms()

    def create_backup(self, volumes_to_snapshot):
        for volume in volumes_to_snapshot:
            create_snapshot(volume)

    def apply_retention_policy(self):
        potential_machines_to_backup = find_all_machines_with_backup_set_to_true()
        all_backups = melt_snapshots_and_vms()
        machines_to_snapshot = get_volumes_to_backup(
            potential_machines_to_backup, all_backups
        )
        volumes_to_snapshot = [machine["VolumeId"] for machine in machines_to_snapshot]
        return volumes_to_snapshot

    def apply_cleaning_policy(self):
        all_snaps_to_remove = []
        removal_7_days = snapshots_to_delete(backups_younger_than_7_days())
        removal_weeks = snapshots_to_delete(backups_older_than_7_days())
        all_snaps_to_remove.append(removal_7_days)
        all_snaps_to_remove.append(removal_weeks)
        final_cleaning_list = [
            item for sublist in all_snaps_to_remove for item in sublist
        ]
        return final_cleaning_list

    def clean_backups(self, snapshotids: list = None):
        for snapshotid in snapshotids:
            delete_snapshot(snapshotid)

bm_instance = BackupManager()
bm_instance._set_bm_attribs_()

# Answer 2
bm_instance.create_backup(volumes_to_snapshot=bm_instance.apply_retention_policy())

# Answer 3
bm_instance.clean_backups(snapshotids=bm_instance.apply_cleaning_policy())