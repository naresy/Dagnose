# Run Dagnose on AWS, Google Cloud or Azure

Dagnose is one long-running container (`dagnose worker`) that needs:

- **one replica only**: its queue is a small SQLite file, so never run two workers on the
  same data;
- **a small persistent volume at `/data`**, so a restart does not forget which failures
  it already handled (without it, a restart looks back again and may diagnose twice);
- **network access** to the Airflow API, the model (Claude API, Bedrock, Vertex AI or
  Foundry) and your chat / email;
- optionally a health check: set `DAGNOSE_HEALTH_PORT=8080` (or `PORT`) and probe
  `GET /healthz`.

How to log into managed Airflow and run the model in your own cloud is in
[install.md](install.md#managed-airflow-aws-google-cloud-azure). Check any setup with
`dagnose check` (as a one-off task or `kubectl exec`).

## Kubernetes (EKS, GKE, AKS)

[`deploy/kubernetes/dagnose.yaml`](../deploy/kubernetes/dagnose.yaml): ServiceAccount,
1 Gi volume, one-replica Deployment (`Recreate`), non-root, read-only root filesystem,
`/healthz` probes. Settings come from a Secret:

```bash
kubectl create namespace dagnose
kubectl -n dagnose create secret generic dagnose-env --from-env-file=dagnose.env
kubectl -n dagnose apply -f deploy/kubernetes/dagnose.yaml
kubectl -n dagnose exec deploy/dagnose -- dagnose check
```

For keyless cloud access, annotate the ServiceAccount: **EKS** IRSA or Pod Identity
(MWAA `InvokeRestApi`, Bedrock), **GKE** Workload Identity (Composer, Vertex AI), **AKS**
Workload Identity (Entra ID, Foundry).

## AWS: ECS on Fargate

[`deploy/aws/ecs-task-definition.json`](../deploy/aws/ecs-task-definition.json) is set
up for MWAA + Bedrock, Slack webhook from Secrets Manager, logs in CloudWatch, state on
EFS. Replace the `ACCOUNT_ID`, `REGION`, `ENVIRONMENT_NAME` and `fs-` placeholders, then:

```bash
aws ecs register-task-definition --cli-input-json file://deploy/aws/ecs-task-definition.json
aws ecs create-service --cluster CLUSTER --service-name dagnose --task-definition dagnose \
  --desired-count 1 --launch-type FARGATE \
  --deployment-configuration "minimumHealthyPercent=0,maximumPercent=100" \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-...],securityGroups=[sg-...]}"
```

- `minimumHealthyPercent=0, maximumPercent=100` keeps it to one task during deploys.
- **Task role**: `airflow:InvokeRestApi` (and `airflow:GetEnvironment`) for MWAA,
  `bedrock:InvokeModel` for Claude on Bedrock. **Execution role**: pull the image, read
  the Secrets Manager secret, write logs.
- A **private** MWAA webserver: run the task in the MWAA VPC's subnets.

## Google Cloud

- **GKE (recommended)**: the Kubernetes manifest above with Workload Identity.
- **Compute Engine**: a small VM running the container with a persistent disk at `/data`.
- **Cloud Run**: possible but not ideal. It needs the health port (Cloud Run sets `PORT`,
  which Dagnose uses), **CPU always allocated**, **min and max instances 1**. Its disk is
  not persistent, so a restart re-reads the last hour and can diagnose a failure twice;
  a Cloud Storage volume is not suitable for the SQLite queue.

The service account needs `roles/composer.user` (Composer) and `roles/aiplatform.user`
(Claude on Vertex AI).

## Azure

- **AKS (recommended)**: the Kubernetes manifest above with Workload Identity.
- **Azure Container Apps**: min and max replicas **1**, ingress off or internal with the
  health probe on `DAGNOSE_HEALTH_PORT`, an Azure Files volume at `/data` (fine with one
  replica), a managed identity for Entra ID / Foundry, and settings as secrets.

## Status

The manifest is validated against the Kubernetes schemas (kubeconform) and the image is
tested with a read-only root filesystem and the health endpoint. Deployments on each
cloud have not been run end to end yet.
