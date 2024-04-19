import boto3
from dagster_pipes import PipesContext, open_dagster_pipes
from time import sleep

def submit_spark_job_to_emr(cluster_id, job_name, spark_script_path, region, context):#, s3_output_path):
    """
    Submit a Spark job to an AWS EMR cluster.
    :param cluster_id: The ID of the EMR cluster.
    :param job_name: The name of the job.
    :param spark_script_path: The S3 path to the Spark script.
    :param s3_output_path: The S3 path for the output of the Spark job.
    """
    try:
        client = boto3.client('emr', region_name=region)
    except Exception as e:
        print(f'Error initializing EMR client: {e}')

    step = {
    'Name': job_name,
    'ActionOnFailure': 'CANCEL_AND_WAIT',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            '/usr/lib/spark/bin/spark-submit',
            '--master', 'yarn',
            '--deploy-mode', 'client',
            '--conf', 'spark.app.name=YelpReviews',
            # Add other Spark configurations as needed, e.g., '--conf', 'spark.executor.memory=4g',
            spark_script_path,
            #s3_output_path  # Assuming this is an argument your Spark application needs
        ]
    }
    }

    try:
        response = client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
        step_id = response['StepIds'][0]
        context.log.info(f"Submitted job with step ID: {step_id}")

        # Wait for the step to complete
        while True:
            step_status = client.describe_step(ClusterId=cluster_id, StepId=step_id)
            state = step_status['Step']['Status']['State']
            if state in ['COMPLETED', 'FAILED', 'CANCELLED']:
                context.log.info(f"Step {step_id} completed with state: {state}")
                break
            else:
                context.log.info(f"Step {step_id} is still running with state: {state}")
                sleep(30)  # Check every 30 seconds

        return step_id
    
    except Exception as e:
        context.log.error(f'Error submitting or monitoring job step: {e}')
        raise

    
def main():
    context = PipesContext.get()

    cluster_id = context.get_extra('cluster_id')
    job_name = context.get_extra('job_name')
    s3_spark_code_path = context.get_extra('s3_spark_code_path')
    region = context.get_extra('region')

    context.log.info(f"processing cluster_id: {cluster_id}")
    
    step_id = submit_spark_job_to_emr(cluster_id, job_name, s3_spark_code_path, region, context)
    print(f'Submitted job with step ID: {step_id}')

    return step_id

if __name__ == "__main__":
    with open_dagster_pipes():
        main()

    
    
 