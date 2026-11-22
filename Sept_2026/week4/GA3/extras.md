```
gcloud dataproc clusters create gokulibdcluster \
  --project project-c38127c2-5130-4284-af3 \
  --region asia-south1 \
  --single-node \
  --master-machine-type e2-standard-2 \
  --master-boot-disk-size 50GB \
  --bucket gokul_ibd_bucket \
  --max-idle 30m
```
