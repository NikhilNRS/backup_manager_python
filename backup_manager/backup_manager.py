import asyncio
import logging
import sys

from aws_utils import create_snapshot, delete_snapshot
from cleaning_policy import (backups_older_than_7_days,
                             backups_younger_than_7_days, snapshots_to_delete)
from cli import cli_parser
from list_backup_utils import melt_snapshots_and_vms
from retainment_policy import (find_all_machines_with_backup_set_to_true,
                               get_volumes_to_backup)


class BackupManager:

    """A class for automating backups of virtual machines running on Amazon Web Services (AWS). It
    contains both functionality to make/delete snapshots of the Elastic Block Storage disks attached
    to AWS virtual machines, based on the labels of the virtual machines. In addition to this, it contains
    default retention - and cleaning policies to manage the backups/snapshots."""

    def __init__(self):
        self.list_basic_info = None

    def _set_bm_attribs_(self):
        self.list_basic_info = melt_snapshots_and_vms()

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

    def get_number_of_backups(self):
        return len(self.list_basic_info)

    async def create_backup(self):
        volumes_to_snapshot = self.apply_retention_policy()
        for volume in volumes_to_snapshot:
            self._set_bm_attribs_()
            number_of_backups = len(self.list_basic_info)
            create_snapshot(volume)
            await asyncio.sleep(0.1)
            while True:
                await asyncio.create_task(self._set_bm_attribs_())
                if number_of_backups < len(len(self.list_basic_info)):
                    break
            await asyncio.sleep(5)

    async def clean_backups(self):
        snapshotids = self.apply_cleaning_policy()
        self._set_bm_attribs_()
        number_of_backups = len(self.list_basic_info)
        for snapshotid in snapshotids:
            delete_snapshot(snapshotid)
            await asyncio.sleep(0.1)
            while True:
                await asyncio.create_task(self._set_bm_attribs_())
                if number_of_backups > len(len(self.list_basic_info)):
                    break
            await asyncio.sleep(5)


def main(sys_args):

    bm_instance = BackupManager()
    bm_instance._set_bm_attribs_()

    args = cli_parser(sys_args)

    try:
        # Case Backup-1: list the info of the virtual machines
        if args.option == "instances":
            print(bm_instance.list_basic_info)

        # Case Backup-2: Create snapshot for disks with 'backup' set to 'true'
        elif args.option == "backup":
            asyncio.run(bm_instance.create_backup())

        # Case Backup-3: Remove old backups following retention policy

        elif args.option == "clean":
            asyncio.run(bm_instance.bm_instance.clean_backups())

        else:
            raise Exception(
                f"'{args.option}'option not available, please select from 'instances', 'backup', 'clean'"
            )
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main(sys.argv[1:])
