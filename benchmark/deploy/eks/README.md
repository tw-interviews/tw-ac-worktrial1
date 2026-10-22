# EKS setup for the benchmark

What the cluster needs beyond SandboxManager itself
(`../../../sandboxmanager/deploy-eks.sh`) and the upstream agent-sandbox
controller v1.0.7 with its extensions. Values in `<angle brackets>` are yours.

## Cluster settings used for the published results

- **VPC CNI prefix delegation on**, through the add-on's configuration so EKS add-on
  updates keep it:

  ```sh
  aws eks update-addon --cluster-name <cluster> --addon-name vpc-cni --region <region> \
    --configuration-values '{"env":{"ENABLE_PREFIX_DELEGATION":"true","WARM_IP_TARGET":"64","MINIMUM_IP_TARGET":"64"}}'
  ```

  Every pod gets a VPC IP, and a deleted pod's IP waits 30 s before reuse. With the
  default spare buffer, sandbox churn runs nodes out of free IPs and pod creation
  fails ("failed to assign an IP address to container") until retried. Nodes created
  **before** prefix delegation is enabled stay limited to their old IPs (the small
  results ran on such nodes, which is why their ceiling is ~2–2.5 calls/s): create
  the benchmark node groups after enabling it.
- **NetworkPolicy enforcement off** (the VPC CNI default). The benchmark's sandboxes only
  sleep; enforcement would add per-pod policy setup to every arm.
- **metrics-server running** (`kubectl top`). If its add-on configuration pins it to
  nodes that don't exist (e.g. a Karpenter `nodeSelector`), clear it:
  `aws eks update-addon ... --addon-name metrics-server --configuration-values '{}'`.

## Node groups for the large profile

Sized for ~2048 sandboxes: one sandbox requests 10m CPU / 64Mi (limit 250m / 128Mi).
On-demand vCPU quota needed: ~200 (check *Running On-Demand Standard instances* in
Service Quotas first).

1. A launch template that raises the kubelet's pod limit to 110, gives a 30 GB gp3 disk,
   and sets the metadata-service hop limit to **1** (2 lets any pod read the node
   role's credentials). User data (MIME multipart, AL2023 `nodeadm`):

   ```yaml
   apiVersion: node.eks.aws/v1alpha1
   kind: NodeConfig
   spec:
     kubelet:
       config:
         maxPods: 110
   ```

2. The sandbox nodes and the load-generator node:

   ```sh
   aws eks create-nodegroup --region <region> --cluster-name <cluster> --nodegroup-name bench-sandbox \
     --node-role <node-role-arn> --subnets <subnet-a> <subnet-b> <subnet-c> \
     --instance-types c7i.2xlarge --ami-type AL2023_x86_64_STANDARD \
     --scaling-config minSize=0,maxSize=20,desiredSize=10 \
     --launch-template id=<launch-template-id>,version=<version> --labels bench-role=sandbox

   aws eks create-nodegroup --region <region> --cluster-name <cluster> --nodegroup-name bench-loadgen \
     --node-role <node-role-arn> --subnets <subnet-a> <subnet-b> <subnet-c> \
     --instance-types c7i.4xlarge --ami-type AL2023_x86_64_STANDARD --disk-size 30 \
     --scaling-config minSize=0,maxSize=1,desiredSize=1 \
     --labels bench-role=loadgen --taints key=bench-role,value=loadgen,effect=NO_SCHEDULE
   ```

   In zsh, pass the subnet IDs as separate words (an unquoted `$SUBNETS` variable is
   not split). Check that the new nodes report 110 allocatable pods; then run
   `../deploy.sh pin`.

3. Delete both node groups when you're done.
