import sys

from acceptance import post

request = {"batch_id": sys.argv[2], "pipeline_version": "v1"}
print(post("/internal/quality" if sys.argv[1] == "validate-data" else "/datasets/export", request))
