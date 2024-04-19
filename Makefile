# Makefile

ifeq ($(OS),Windows_NT)
    detected_OS := Windows
else
    detected_OS := $(shell uname)
endif

# Define the required Python version
REQUIRED_PYTHON_VERSION=Python 3.10

# Target to check Python version
check-python-version:
	@echo "Checking Python version..."
	@python --version 2>&1 | grep '$(REQUIRED_PYTHON_VERSION)' > /dev/null || (echo "Python 3.10 is required. Please install it to proceed." && exit 1)

# Setup the environment
# Use the check as a prerequisite for other targets
setup: check-python-version
ifeq ($(detected_OS),Windows)
    python3.10 -m venv myenv
    myenv\Scripts\activate
    pip install -r requirements.txt
else
    python3.10 -m venv myenv
	chmod +x ./myenv/bin/activate
    . myenv/bin/activate
endif

# Check for AWS CLI
check-aws-cli:
    @which aws > /dev/null || (echo "AWS CLI not installed. Please install it from https://aws.amazon.com/cli/" && exit 1)

# Configure AWS CLI
aws-setup:
    @echo "Running AWS CLI configuration..."
    @echo "Please enter your AWS Access Key ID, Secret Access Key, Default region name (e.g., us-east-1), and Default output format (e.g., json)."
    aws configure

# Create IAM roles and policies
create-iam-roles:
    @echo "Creating IAM roles and attaching policies..."
    aws iam create-role --role-name MyDagsterEMRServiceRole --assume-role-policy-document file://emr-trust-policy.json
    aws iam attach-role-policy --role-name MyDagsterEMRServiceRole --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole
    aws iam attach-role-policy --role-name MyDagsterEMRServiceRole --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role
    aws iam create-instance-profile --instance-profile-name DagsterEMRInstanceProfile
    aws iam add-role-to-instance-profile --instance-profile-name DagsterEMRInstanceProfile --role-name MyDagsterEMRServiceRole

# Fetch and configure VPC Subnet ID
configure-vpc:
    @echo "Fetching VPC and Subnet IDs..."
    $(eval VPC_ID := $(shell aws ec2 describe-vpcs --filters Name=is-default,Values=true --query "Vpcs[0].VpcId" --output text))
    $(eval SUBNET_ID := $(shell aws ec2 describe-subnets --filters "Name=vpc-id,Values=${VPC_ID}" --query "Subnets[0].SubnetId" --output text))
    @echo "Default VPC ID: ${VPC_ID}"
    @echo "Default Subnet ID: ${SUBNET_ID}"
    @echo "Please manually set these IDs in your configuration files or environment variables as needed."

# Clean up resources
clean:
    deactivate
    rm -rf myenv

.PHONY: setup aws-setup create-iam-roles configure-vpc clean
