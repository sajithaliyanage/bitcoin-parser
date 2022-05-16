# Airflow Deployment

Install Airflow:

```
helm install airflow-cluster   airflow-stable/airflow  --namespace airflow-cluster -f values.yaml --version "8.X.X" --create-namespace
```

Install Airflow PVC:

```
kubectl apply -f pvc.yaml
```

Install Airflow Ingress:

```
kubectl apply -f ingress.yaml
```
