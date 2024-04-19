#!/bin/bash
aws s3 cp s3://your-bucket/emr-resources/requirements.txt /home/hadoop/requirements.txt
sudo pip install -r /home/hadoop/requirements.txt
