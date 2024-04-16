

## Links
- ['Using Dagster with Spark'](https://docs.dagster.io/integrations/spark#asset-accepts-and-produces-dataframes-or-rdds)
- ['emr_pyspark_launcher'](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-aws/dagster_aws/emr/pyspark_step_launcher.py)
- ['create-default-roles for EMR Cluster'](https://docs.aws.amazon.com/cli/latest/reference/emr/create-default-roles.html#create-default-roles)
- ['AWS S3 multipart upload documentation'](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html#sdksupportformpu)




## Getting started

Let's start by creating a virtual python environment. 

```bash
python3.10 -m venv myenv
```

```bash
cd myenv/bin
chmod +x activate
```

Now switch the python interpreter to our newly created virtual python environment

```bash
..\myenv\Scripts\Activate
```

First, install your Dagster code location as a Python package by running the command below in your terminal. By using the --editable (`-e`) flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Duplicate the `.env.example` file and rename it to `.env`.

Then, start the Dagster UI web server:

```bash
dagster dev

(old) dagster dev -d dagster_codespaces -m dagster_capstone
```

Open http://localhost:3000 with your browser to see the project.


## Deploy on Dagster Cloud

The easiest way to deploy your Dagster project is to use Dagster Cloud.

Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud) to learn more.
