# Spanner Graph Data Ingestion Pipeline

This module implements a Dataflow pipeline that loads Spanner DB with StatVar observations and graph reading from GCS cache. The Pipeline can be run in local mode for testing (DirectRunner) or in the cloud (DataflowRunner).

'''
mvn -Pdataflow-runner compile exec:java -pl ingestion -am -Dexec.mainClass=org.datacommons.IngestionPipeline -Dexec.args="--project=<project-id> --gcpTempLocation=<gs://path> --runner=DataflowRunner --region=<region>  --projectId=<project-id> --spannerInstanceId=<instance-id> --spannerDatabaseId=<database-id> --cacheType=<observation/graph> --importGroupList=auto1d,auto1w,auto2w --storageBucketId=<bucket-id>"
'''
