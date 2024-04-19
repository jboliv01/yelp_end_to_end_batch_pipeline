import boto3
from dagster_pipes import PipesContext, open_dagster_pipes
from time import sleep

def create_emr_cluster(region, s3_bucket_prefix, vpc_default_subnet_id, context):
    """
    Create an AWS EMR cluster in the specified region with predefined settings.

    :param region: AWS region to create the EMR cluster in.
    :return: The ID of the created EMR cluster.
    """
    emr_client = boto3.client('emr', region_name=region)
    bootstrap_path = f"{s3_bucket_prefix}emr-resources/install-boto3.sh"
    log_path = f"{s3_bucket_prefix}emr-resources/logs/"


    cluster_response = emr_client.run_job_flow(
        Name="My cluster",
        LogUri=log_path,
        ReleaseLabel="emr-7.0.0",
        Instances={
            'MasterInstanceType': 'm5.xlarge',
            'SlaveInstanceType': 'm5.xlarge',
            'InstanceCount': 3,
            'Ec2SubnetId': vpc_default_subnet_id, #subnet-05cc5eb5acd08207f
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
                    'Path': bootstrap_path,
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
    s3_bucket_prefix = context.get_extra('s3_bucket_prefix')
    vpc_default_subnet_id = context.get_extra('vpc_default_subnet_id')
    cluster_id = create_emr_cluster(region, s3_bucket_prefix, vpc_default_subnet_id, context)

    context.report_asset_materialization(asset_key='emr_cluster', 
                                         metadata={"cluster_id": cluster_id})


if __name__ == '__main__':
    with open_dagster_pipes():
        main()