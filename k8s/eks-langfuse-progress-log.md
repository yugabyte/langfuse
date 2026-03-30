# EKS Langfuse Setup Progress Log

Date: 2026-03-27
Namespace: `meko-langfuse`
Cluster context: `arn:aws:eks:us-east-2:161908593886:cluster/meko_dev_eks`

This file documents what was done so far, with command outputs captured from terminal history and command runs in this session.

---

## 1) AWS CLI auth and profile setup

### Command
`aws configure sso`

### Output summary
- SSO login completed successfully.
- Account selected: `161908593886`
- Role selected: `AdministratorAccess`
- Default region set: `us-east-2`
- Profile set to `default`

### Verification commands
`aws s3 ls --profile default` and `aws s3 ls`

### Output observed
- `bedrock-agentcore-codebuild-sources-161908593886-us-west-2`
- `languse-meko-bucket` (typo bucket, fixed later)

---

## 2) kubectl installation and EKS kubeconfig setup

### Commands
- `curl -LO ".../kubectl"`
- `curl -LO ".../kubectl.sha256"`
- `echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check`
- `sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl`
- `kubectl version --client`
- `aws eks update-kubeconfig --region us-east-2 --name meko_dev_eks`

### Output observed
- Checksum: `kubectl: OK`
- Client version: `v1.35.3`
- Context added to kubeconfig:
  `arn:aws:eks:us-east-2:161908593886:cluster/meko_dev_eks`

---

## 3) Namespace creation

### Commands
- `kubectl get ns`
- `kubectl create namespace meko-langfuse`

### Output observed
- Existing namespaces listed successfully.
- Namespace creation:
  `namespace/meko-langfuse created`

---

## 4) Current context and default namespace setup

### Commands and outputs

#### `kubectl config current-context`
Output:
`arn:aws:eks:us-east-2:161908593886:cluster/meko_dev_eks`

#### `kubectl config set-context --current --namespace=meko-langfuse`
Output:
`Context "arn:aws:eks:us-east-2:161908593886:cluster/meko_dev_eks" modified.`

#### `kubectl config view --minify --output 'jsonpath={..namespace}'; echo`
Output:
`meko-langfuse`

---

## 5) S3 bucket correction (typo fix)

Initial state had bucket typo: `languse-meko-bucket`.

### Commands run
- `aws s3 ls`
- `aws s3 rb s3://languse-meko-bucket --force`
- `aws s3api create-bucket --bucket langfuse-meko-bucket --region us-east-2 --create-bucket-configuration LocationConstraint=us-east-2`
- `aws s3 ls`

### Output observed
- Old typo bucket removed:
  `remove_bucket: languse-meko-bucket`
- New bucket created:
  `langfuse-meko-bucket`
- Final list shows corrected bucket name.

---

## 6) Helm installation and Valkey deployment (official chart)

### Helm install
#### Commands
- `curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-4`
- `chmod 700 get_helm.sh`
- `./get_helm.sh`
- `helm version`

#### Output observed
- Helm installed to `/usr/local/bin/helm`
- Version:
  `v4.1.3`

### Add official Valkey chart repo
#### Commands
- `helm repo add valkey https://valkey.io/valkey-helm/`
- `helm repo update`

#### Output observed
- `"valkey" has been added to your repositories`
- `Successfully got an update from the "valkey" chart repository`

### Deploy Valkey
#### Command
`helm install valkey valkey/valkey -n meko-langfuse`

#### Output observed
- `STATUS: deployed`
- `Chart: valkey 0.9.3`
- `App version: 9.0.1`
- Service name: `valkey`
- Port: `6379`
- Auth: disabled (per chart notes)

### Validate Valkey workload
#### Commands
- `kubectl get pods -n meko-langfuse`
- `kubectl get svc -n meko-langfuse`

#### Output observed
- Pod:
  `valkey-6c49d8689b-k5tzc   1/1   Running`
- Service:
  `valkey   ClusterIP   172.20.39.207   6379/TCP`

---

## 7) Langfuse env files updated for AWS S3 + Valkey

Files updated:
- `k8s/langfuse.config.env`
- `k8s/langfuse.secrets.env`

### Changes made
- Switched S3 target to AWS bucket:
  - Bucket: `langfuse-meko-bucket`
  - Region: `us-east-2`
- Removed MinIO-specific endpoint/path-style settings.
- Updated Redis host for in-cluster Valkey:
  - `REDIS_HOST=valkey`
- Set Redis auth empty (because current Valkey install has auth disabled):
  - `REDIS_AUTH=`
- Inserted AWS key variables in env files.

### Security note
AWS credentials were shared in chat during setup; they should be treated as compromised and rotated.

---

## 8) Current status against requested task list

Requested:
1. Configure Langfuse to use AWS S3
2. Create namespace on EKS
3. Deploy Valkey in namespace
4. Deploy Langfuse YAML in namespace

Status:
- [x] (1) AWS S3 config prepared in `k8s/*.env`
- [x] (2) Namespace `meko-langfuse` created and set as default
- [x] (3) Valkey deployed and verified
- [x] (4) Langfuse deployment to namespace completed

---

## 9) ECR image verification and deployment YAML image update

### Intent
Use newly pushed image versions (immutable tags) instead of `latest-*` tags or local images.

### Command run
`aws ecr describe-images --repository-name meko/meko-langfuse-releases --region us-east-2 --query 'imageDetails[].{pushed:imagePushedAt,tags:imageTags,digest:imageDigest}' --output json`

### Output observed (latest relevant tags)
- Web image:
  - Tag: `0.0.2-20260327-web`
  - Also tagged as: `latest-web`
  - Digest: `sha256:418f6f1d506ea308afe420cbc2c2817b9bbf55954baaae33eb4a9ae8c40fad99`
  - Pushed at: `2026-03-27T10:45:55.748000+00:00`
- Worker image:
  - Tag: `0.0.2-20260327-worker`
  - Also tagged as: `latest-worker`
  - Digest: `sha256:86c97f538b90c6b3fd86666b68675287d78abeacc58c35aa0879a247cf1d4db3`
  - Pushed at: `2026-03-27T10:47:07.922000+00:00`

### File updated
`k8s/langfuse-web-worker-single-pod.yaml`

### Change made
- Web container image changed to:
  `161908593886.dkr.ecr.us-east-2.amazonaws.com/meko/meko-langfuse-releases:0.0.2-20260327-web`
- Worker container image changed to:
  `161908593886.dkr.ecr.us-east-2.amazonaws.com/meko/meko-langfuse-releases:0.0.2-20260327-worker`

### Why this change
- EKS cannot run local images like `langfuse-web:local`.
- Using immutable tags prevents accidental drift from `latest-*`.

---

## 10) Langfuse deployment execution

### Commands run
1. `kubectl -n meko-langfuse create configmap langfuse-config --from-env-file=k8s/langfuse.config.env --dry-run=client -o yaml | kubectl apply -f -`
2. `kubectl -n meko-langfuse create secret generic langfuse-secrets --from-env-file=k8s/langfuse.secrets.env --dry-run=client -o yaml | kubectl apply -f -`
3. `kubectl -n meko-langfuse apply -f k8s/langfuse-web-worker-single-pod.yaml`
4. `kubectl -n meko-langfuse rollout status deployment/langfuse-web-worker --timeout=300s`

### Output observed
- `configmap/langfuse-config created`
- `secret/langfuse-secrets created`
- `deployment.apps/langfuse-web-worker created`
- `service/langfuse-web created`
- Rollout status:
  - `Waiting for deployment "langfuse-web-worker" rollout to finish: 0 of 1 updated replicas are available...`
  - `deployment "langfuse-web-worker" successfully rolled out`

### Result
Langfuse deployment is live in namespace `meko-langfuse` using:
- AWS S3 bucket `langfuse-meko-bucket`
- Valkey service `valkey:6379`
- ECR images tagged `0.0.2-20260327-web` and `0.0.2-20260327-worker`

---

## 11) Final status

All requested tasks are completed:

- [x] Configure Langfuse to use AWS S3
- [x] Create a namespace on EKS
- [x] Deploy Valkey in the namespace
- [x] Deploy Langfuse in the namespace using generated YAML

---

## 12) Post-deploy verification (kubectl + AWS UI)

### Commands run
- `kubectl config current-context`
- `kubectl get ns`
- `kubectl -n meko-langfuse get all`

### Output observed
- Context:
  - `arn:aws:eks:us-east-2:161908593886:cluster/meko_dev_eks`
- Namespace:
  - `meko-langfuse` is `Active`
- Workloads/services:
  - `pod/langfuse-web-worker-...` => `2/2 Running`
  - `pod/valkey-...` => `1/1 Running`
  - `service/langfuse-web` => `ClusterIP ... 3000/TCP`
  - `service/valkey` => `ClusterIP ... 6379/TCP`
  - `deployment/langfuse-web-worker` => `1/1 Available`
  - `deployment/valkey` => `1/1 Available`

### AWS UI confirmation
- EKS cluster `meko_dev_eks` shows namespace `meko-langfuse` in `Ready` state.
- Deployment `langfuse-web-worker` visible in namespace-scoped Workloads view.

---

## 13) Access notes and fixes

### Why app was not directly visible via public URL
- `langfuse-web` service type is `ClusterIP` (internal only), so no public endpoint is auto-created.

### Port-forward issue encountered
- `kubectl ... port-forward ... 3000:3000` failed because local port 3000 was already in use.
- Use alternate local port, e.g. `3001:3000`.

### Auth redirect issue encountered
- Redirect to `http://localhost:5000` happened due to:
  - `NEXTAUTH_URL=http://localhost:5000` in `k8s/langfuse.config.env`
- Fix by setting `NEXTAUTH_URL` to actual access URL (e.g. `http://localhost:3001` while port-forwarding), then re-applying ConfigMap and restarting deployment.

### Sign-in bootstrap clarification
- `LANGFUSE_INIT_*` variables create bootstrap org/project/user data.
- They do not create an auto-login browser session cookie.

---

## 14) Local side cleanup

To avoid local port/app conflicts during validation, docker compose stack in `~/code/meko/client` was stopped:

- `docker compose -f docker-compose.containers.yml down`

Removed:
- `meko-ui`
- `meko-proxy`
- `client_meko_net` network

