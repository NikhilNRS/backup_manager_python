import boto3

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


def fetch_snapshots_per_volume(volume_id):
    return ec2_client.describe_snapshots(
        Filters=[{"Name": "volume-id", "Values": volume_id}]
    )
