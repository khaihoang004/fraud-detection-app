resource "kubernetes_deployment" "producer" {
  metadata {
    name = "fraud-producer"
    labels = {
      app = "fraud-producer"
    }
  }

  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "fraud-producer"
      }
    }
    template {
      metadata {
        labels = {
          app = "fraud-producer"
        }
      }
      spec {
        container {
          image             = "fraud-producer:latest"
          name              = "producer"
          image_pull_policy = "Never" # Important for local Minikube images

          env {
            name  = "KAFKA_BROKER"
            value = "kafka.default.svc.cluster.local:9092" # Standard internal DNS for Bitnami Kafka
          }

          # Mount data volume if necessary, or assuming data is built into image
          volume_mount {
            name       = "data-volume"
            mount_path = "/app/data"
          }
        }

        volume {
          name = "data-volume"
          host_path {
            path = "/data/fraud-app" # Placeholder, might need adjustment based on where user mounts data in minikube
            type = "DirectoryOrCreate"
          }
        }
      }
    }
  }
}

resource "kubernetes_deployment" "consumer" {
  metadata {
    name = "fraud-consumer"
    labels = {
      app = "fraud-consumer"
    }
  }

  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "fraud-consumer"
      }
    }
    template {
      metadata {
        labels = {
          app = "fraud-consumer"
        }
      }
      spec {
        container {
          image             = "fraud-consumer:latest"
          name              = "consumer"
          image_pull_policy = "Never"

          env {
            name  = "KAFKA_BROKER"
            value = "kafka.default.svc.cluster.local:9092"
          }
        }
      }
    }
  }
}
