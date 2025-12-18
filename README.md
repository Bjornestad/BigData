# BigData

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
```

```bash
helm dependency build ./energy-platform
```

```bash
helm install energy-stack ./energy-platform --namespace bd-bd-gr-08
```

```bash
kubectl port-forward svc/kafka-ui 8080:8080 -n bd-bd-gr-08
```
