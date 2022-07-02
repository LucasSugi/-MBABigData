# Databricks notebook source
# Mount
aws_bucket_name = ""
mount_name = "microdados_gov"
dbutils.fs.mount("s3a://{}".format(aws_bucket_name), "/mnt/{}".format(mount_name))