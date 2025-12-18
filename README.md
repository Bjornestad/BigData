# BigData

helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

helm dependency build ./energy-platform

helm install energy-stack ./energy-platform --namespace bd-bd-gr-08

kubectl port-forward svc/kafka-ui 8080:8080 -n bd-bd-gr-08
