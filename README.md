# toshi-bitcoin-parser

![Release](https://github.com/toshi-qcri/toshi-bitcoin-parser/workflows/Release/badge.svg?branch=master&event=push)
![Test Release](https://github.com/toshi-qcri/toshi-bitcoin-parser/workflows/Test%20Release/badge.svg?branch=develop&event=push)

# Setup

### Setup K8S Cluster
You can deploy a Kubernetes cluster on a local machine, cloud, on-prem datacenter, or choose a managed Kubernetes cluster. You can also create custom solutions across a wide range of cloud providers, or bare metal environments.

**Prerequisites**
1. Multiple servers running Linux OS(1 Master Node, 3 Worker Nodes). It is recommended that your Master Node have at least 2 CPUs, though this is not a strict requirement.
2. Internet connectivity on all nodes. Kubernetes and Docker packages will be fetched from the repository.
3. An account with sudo or root privileges.

Clone BlockStack repository & install k8s dependencies using bootstrap.sh on all nodes
```sh
git clone https://github.com/cibr-qcri/bitcoin-parser.git
cd bitcoin-parser/k8s
sh bootstrapper.sh
```

Inorder to initialize k8s master run this only on master node
```sh
kubeadm init --config kubeadm-config.yaml
```

Export KUBECONFIG environment variable to access the cluser via `kubectl`
```sh
export KUBECONFIG=/etc/kubernetes/admin.conf
```

Deploying the network cluster is a highly flexible process depending on your needs and there are many options available. Since we want to keep our installation as simple as possible, we will use Flannel plugin which does not require any configuration or extra code and it provides one IP address per pod which is great for us
```sh
kubectl apply -f "https://raw.githubusercontent.com/coreos/flannel/master/Documentation/kube-flannel.yml"
```

Having initialized Kubernetes successfully, you will need to allow your user to start using the cluster. In our case, we want to run this installation as the root user, therefore we will go ahead and run these commands as root. You can change to a sudo enabled user you prefer and run the below using sudo(on master node).
To use root, run:
```sh
mkdir -p "$HOME/.kube"
cp -i "/etc/kubernetes/admin.conf" "$HOME/.kube/config"
chown "$(id -u):$(id -g)" "$HOME/.kube/config"
```

Install Helm package manager on master node
```sh
chmod +x get_helm.sh
sh get_helm.sh
```

Add official helm repository
```sh
helm repo add stable https://charts.helm.sh/stable
```

Now you have initialized k8s master node and installed dependencies on all nodes. However at this point k8s cluster is not created since worker nodes have not been recognized by the master. In order to join nodes to the k8s cluster run following command on master and copy the output from above command and run it on all worker nodes that needs to be joined to the k8s cluster..
```sh
kubeadm token create --print-join-command
```

Now k8s cluster is initialized and you can list nodes by running following command on the master node
```sh
kubectl get nodes
```

# Setup NFS Dynamic Provisioner

Inorder to create a NFS provisioner for the k8s cluster run following command on the master node.

Create k8s resources for role based binding and storage class scripts by running
```sh
kubectl create -f nfs-rbac.yaml
kubectl create -f nfs-storage-class.yaml
```

## Deploying NFS Provisioner

Now let’s deploy the nfs provisioner. But first we’ll need to edit the nfs_deployment.yaml file. In this file we’ll need to specify the IP Address of our NFS Server.

```yaml
kind: Deployment
apiVersion: apps/v1
metadata:
  name: nfs-client-provisioner
spec:
  selector:
    matchLabels:
      app: nfs-client-provisioner
  replicas: 1
  strategy:
    type: Recreate
  template:
    metadata:
      labels:
        app: nfs-client-provisioner
    spec:
      serviceAccountName: nfs-client-provisioner
      containers:
        - name: nfs-client-provisioner
          image: quay.io/external_storage/nfs-client-provisioner:latest
          volumeMounts:
            - name: nfs-client-root
              mountPath: /persistentvolumes
          env:
            - name: PROVISIONER_NAME
              value: example.com/nfs # SAME AS PROVISONER NAME VALUE IN STORAGECLASS
            - name: NFS_SERVER
              value: <<NFS Server IP>> # Ip of the NFS SERVER
            - name: NFS_PATH
              value: /var/nfs/general # path to nfs directory setup
      volumes:
        - name: nfs-client-root
          nfs:
            server: <<NFS Server IP>> # Ip of the NFS SERVER
            path: /var/nfs/general # path to nfs directory setup
```

Create K8s resource by running
```sh
kubectl create -f nfs_deployment.yaml
```

# Install Ingress Controller & Resources
To install the Nginx ingress controller helm chart with the release name nginx-ingress

```sh
helm repo add nginx-stable https://helm.nginx.com/stable
helm update
helm install nginx-ingress nginx-stable/nginx-ingress
```

or 

```sh
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.0.0/deploy/static/provider/baremetal/deploy.yaml
```

### Validate Nginx Controller for kubectl command
```sh
$ kubectl get pods --all-namespaces -l app.kubernetes.io/name=ingress-nginx
NAMESPACE       NAME                                        READY   STATUS      RESTARTS   AGE
ingress-nginx   ingress-nginx-admission-create-wb4rm        0/1     Completed   0          17m
ingress-nginx   ingress-nginx-admission-patch-dqsnv         0/1     Completed   2          17m
ingress-nginx   ingress-nginx-controller-74fd5565fb-lw6nq   1/1     Running     0          17m


$ kubectl get services ingress-nginx-controller --namespace=ingress-nginx
NAME                       TYPE           CLUSTER-IP    EXTERNAL-IP   PORT(S)                      AGE
ingress-nginx-controller   NodePort       10.21.1.110   10.0.0.3      80:32495/TCP,443:30703/TCP   17m
```

# Install ArangoDB
## Install ArangoDB operator
```sh
kubectl apply -f https://raw.githubusercontent.com/arangodb/kube-arangodb/1.2.6/manifests/arango-crd.yaml
kubectl apply -f https://raw.githubusercontent.com/arangodb/kube-arangodb/1.2.6/manifests/arango-deployment.yaml
kubectl apply -f https://raw.githubusercontent.com/arangodb/kube-arangodb/1.2.6/manifests/arango-storage.yaml
kubectl apply -f https://raw.githubusercontent.com/arangodb/kube-arangodb/1.2.6/manifests/arango-deployment-replication.yaml
```

If you are using new Kubernetes version (> 1.20.0) please refer [this link](https://github.com/kubernetes-sigs/nfs-subdir-external-provisioner/issues/25) to sort out the PVC pending state issue.


## Install ArangoDB Deployment
```sh
kubectl apply -f charts/arangodb/arangodb_deployment.yaml
```

# Install Greenplum DB
```sh
cd charts/greenplum
helm install greenplum-operator operator/
helm install greenplum-database database/
```
