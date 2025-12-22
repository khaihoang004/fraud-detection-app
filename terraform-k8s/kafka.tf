resource "helm_release" "kafka" {
  name       = "kafka"
  repository = "https://charts.bitnami.com/bitnami"
  chart      = "kafka"
  namespace  = "default"

  set = [
    {
      name  = "listeners.client.protocol"
      value = "PLAINTEXT"
    },
    {
      name  = "provisioning.enabled"
      value = "true"
    },
    {
      name  = "provisioning.topics[0].name"
      value = "fraud-stream"
    },
    {
      name  = "provisioning.topics[0].partitions"
      value = "1"
    },
    {
      name  = "persistence.enabled"
      value = "false" # For easier testing on Minikube
    },
    {
      name  = "zookeeper.persistence.enabled"
      value = "false"
    }
  ]
}
