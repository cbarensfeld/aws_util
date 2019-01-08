#!/usr/bin/env python3.6
import boto3
import datetime
import logging

client = boto3.client('ec2')

def lambda_handler(event, context):
    retention_tag = 'retentionPeriod'
    filter = [{'Name': 'tag-key', 'Values': [retention_tag]}]

    # Logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger(__name__)

    # Get volume id and tags
    volumes = []
    describe_volumes = client.describe_volumes(Filters=filter)
    for volume in describe_volumes['Volumes']:
        tags = volume['Tags']
        # Get retention from volume
        for tag in tags:
            if tag['Key'] == retention_tag:
                retention = tag['Value']
        volumes.append({'id' : volume['VolumeId'], 'tags' : tags, 'retention' : retention})

    # Take snapshots
    for volume in volumes:
        if int(volume['retention']) > 0:
            create_snapshot = client.create_snapshot(
                Description='Created from persistent-volume-backup',
                VolumeId=volume['id'],
                TagSpecifications=[{'ResourceType': 'snapshot', 'Tags': volume['tags']}]
            )
            logger.info('Creating snapshot %s from %s', create_snapshot['SnapshotId'], create_snapshot['VolumeId'])

    # Get all snapshots
    describe_snapshots = client.describe_snapshots(Filters=filter)
    # Get total number of snapshots per volume
    snapshot_count = {}
    for volume in volumes:
        count = client.describe_snapshots(Filters=[{'Name': 'volume-id', 'Values': [volume['id']]}])
        snapshot_count.update({volume['id']: len(count['Snapshots'])})
    # Check if snapshot should be removed
    for snapshot in describe_snapshots['Snapshots']:
        for volume in volumes:
            if volume['id'] == snapshot['VolumeId']:
                # Delete snapshot
                clean_date = datetime.datetime.now() - datetime.timedelta(days=int(volume['retention']))
                if clean_date > snapshot['StartTime'].replace(tzinfo=None) and int(snapshot_count[volume['id']]) > int(volume['retention']):
                    logger.info('Deleting snapshot %s from volume %s, older than %s days', snapshot['SnapshotId'], snapshot['VolumeId'], volume['retention'])
                    delete_snapshot = client.delete_snapshot(SnapshotId=snapshot['SnapshotId'])

    return 'Snapshot job complete'
