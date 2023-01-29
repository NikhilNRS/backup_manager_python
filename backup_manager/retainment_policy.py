from date_utils import before_today
from list_backup_utils import parse_vms, fetch_instances


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
