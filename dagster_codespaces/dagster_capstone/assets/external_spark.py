import boto3
import argparse

def submit_spark_job_to_emr(cluster_id, job_name, spark_script_path, region):#, s3_output_path):
    """
    Submit a Spark job to an AWS EMR cluster.
    :param cluster_id: The ID of the EMR cluster.
    :param job_name: The name of the job.
    :param spark_script_path: The S3 path to the Spark script.
    :param s3_output_path: The S3 path for the output of the Spark job.
    """
    client = boto3.client('emr', region_name=region)
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
    response = client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    return response['StepIds']



def main():

    cluster_id = 'j-2YLAXTLFQWW5Z' 
    job_name = 'Pi'
    s3_spark_code_path = 's3://de-capstone-project/emr-resources/spark-code/emr_spark_yelp_reviews.py' 
    s3_output_path = 's3://de-capstone-project/yelp/processed/'
    region = 'us-west-2'

    parser = argparse.ArgumentParser(
                    prog='Parse Spark Job Details',
                    description='What the program does',
                    epilog='Text at the bottom of help')
    
    parser.add_argument('cluster_id', type=str, required=True)
    parser.add_argument('job_name', type=str, required=True) 
    parser.add_argument('s3_spark_code_path', type=str, required=True)
    parser.add_argument('region_name', type=str, requred=True)
    
    args = parser.parse_args()

    cluster_id = args.cluster_id
    job_name = args.job_name
    s3_spark_code_path = args.s3_spark_code_path
    region = args.region_name

    step_id = submit_spark_job_to_emr(cluster_id, job_name, s3_spark_code_path, region)#, s3_output_path)
    print(f'Submitted job with step ID: {step_id}')

    return step_id

if __name__ == "__main__":
    main()
