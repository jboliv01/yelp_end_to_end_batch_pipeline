
# AWS guide to setting up EMR permissions

1. Define a trust policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "elasticmapreduce.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

2. Create the Service Role
```bash
aws iam create-role --role-name MyDagsterEMRServiceRole --assume-role-policy-document file://emr-trust-policy.json
```

3. Attach Policies to the Role
```bash
aws iam attach-role-policy --role-name MyDagsterEMRServiceRole --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole

aws iam attach-role-policy --role-name MyDagsterEMRServiceRole --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role

```

4. Create an Instance Profile
```bash
aws iam create-instance-profile --instance-profile-name DagsterEMRInstanceProfile

```

5. Retrieve the Serivce Role ARN