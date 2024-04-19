import boto3
from dagster_pipes import PipesContext, open_dagster_pipes
from time import sleep

def create_emr_cluster(region, context):
    """
    Create an AWS EMR cluster in the specified region with predefined settings.

    :param region: AWS region to create the EMR cluster in.
    :return: The ID of the created EMR cluster.
    """
    emr_client = boto3.client('emr', region_name=region)

    cluster_response = emr_client.run_job_flow(
        Name="My cluster",
        LogUri="s3://aws-logs-388058924848-us-west-2/elasticmapreduce/",
        ReleaseLabel="emr-7.0.0",
        Instances={
            'MasterInstanceType': 'm5.xlarge',
            'SlaveInstanceType': 'm5.xlarge',
            'InstanceCount': 3,
            'Ec2SubnetId': 'subnet-6762990c', #subnet-05cc5eb5acd08207f
            # 'EmrManagedMasterSecurityGroup': 'sg-0ff600d7a717a3608',
            # 'EmrManagedSlaveSecurityGroup': 'sg-070547519205509be',
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,  # Set to True if you need the cluster to be termination-protected
        },
        ServiceRole="arn:aws:iam::388058924848:role/MyDagsterEMRServiceRole",
        JobFlowRole="EMR_EC2_DefaultRole",
        Applications=[
            {'Name': 'Hadoop'}, {'Name': 'Hive'}, {'Name': 'Spark'},
            {'Name': 'Livy'}, {'Name': 'JupyterEnterpriseGateway'}
        ],
        BootstrapActions=[
            {
                'Name': 'install boto3',
                'ScriptBootstrapAction': {
                    'Path': "s3://de-capstone-project/emr-resources/install-boto3.sh",
                    'Args': []
                }
            }
        ],
        Steps=[],  # Since steps are managed elsewhere, define this as an empty list
        VisibleToAllUsers=True,
        AutoScalingRole="EMR_AutoScaling_DefaultRole",  # Adjust as necessary
        ScaleDownBehavior="TERMINATE_AT_TASK_COMPLETION",
        AutoTerminationPolicy={"IdleTimeout": 900}
    )

    cluster_id = cluster_response['JobFlowId']

    while True:
        cluster_description = emr_client.describe_cluster(ClusterId=cluster_id)
        status = cluster_description['Cluster']['Status']['State']
        if status in ['WAITING', 'RUNNING']:
            context.log.info(f"Cluster is now in {status} state.")
            break
        elif status in ['TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS']:
            raise Exception(f"Cluster entered {status} state.")
        else:
            context.log.info(f"Current cluster state: {status}")
            sleep(30)  # Wait for 30 seconds before checking again

    return cluster_id

def main():
    context = PipesContext.get()
    region = context.get_extra('region')
    cluster_id = create_emr_cluster(region, context)

    context.report_asset_materialization(asset_key='emr_cluster', 
                                         metadata={"cluster_id": cluster_id})


if __name__ == '__main__':
    with open_dagster_pipes():
        main()