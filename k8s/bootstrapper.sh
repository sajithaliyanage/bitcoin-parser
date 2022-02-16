sudo apt-get update

# Disable swap(This only disables swap temporary)
sudo swapoff -a

if [ -x "$(command -v docker)" ]; then
    echo "Update Docker"
else
    sudo apt-get install docker.io && sudo usermod -aG docker "$USER"
	sudo systemctl start docker && sudo systemctl enable docker
fi

if [ -x "$(command -v kubeadm)" ]; then
    echo "Update Kubernetes"
else
    sudo apt-get install -y apt-transport-https
    curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
    sudo apt-add-repository "deb http://apt.kubernetes.io/ kubernetes-xenial main"
    sudo apt-get update && sudo apt-get install -y kubelet kubeadm kubectl
fi
