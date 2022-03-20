#!/bin/bash
bucket="batch-processing-reddit-api"
region="us-east-1"

aws s3api create-bucket --bucket $bucket --region $region
