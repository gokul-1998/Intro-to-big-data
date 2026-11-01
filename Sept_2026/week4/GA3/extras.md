```
gcloud dataproc clusters create CLUSTER_NAME \
  --project AAPKA_PROJECT \
  --region asia-south1 \
  --single-node \
  --master-machine-type e2-standard-2 \
  --master-boot-disk-size 50GB \
  --bucket AAPKA_BUCKET \
  --max-idle 30m
```
