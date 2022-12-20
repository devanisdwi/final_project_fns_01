provider "google" {
  project = var.project_id
  region  = var.region
}

module "composer_project" {
  source          = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/project"
  billing_account = var.billing_account_id
  prefix          = var.prefix
  name            = var.project_id
  iam_additive = {
    "roles/owner" = var.owners
  }
  # Required for Cloud Composer
  policy_boolean = {
    "constraints/compute.requireOsLogin" = false
  }
  services        = [
    "composer.googleapis.com",
    "sourcerepo.googleapis.com",
    "bigquery.googleapis.com",
    "iamcredentials.googleapis.com",
    "pubsub.googleapis.com"
  ]
}

module "composer_sa" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/iam-service-account?ref=v5.1.0"
  project_id = module.composer_project.project_id
  name       = "composer-sa"
  iam_project_roles = {
    (module.composer_project.project_id) = [
      "roles/composer.user",
      "roles/composer.worker",
      "roles/logging.logWriter",
      "roles/monitoring.metricWriter",
      "roles/iam.serviceAccountUser",
      "roles/composer.ServiceAgentV2Ext",
      "roles/bigquery.admin",
      "roles/container.admin",
      "roles/storagetransfer.serviceAgent",
      "roles/pubsub.editor",
      "roles/composer.admin"
    ]
  }
  iam = var.owners != null ? { "roles/iam.serviceAccountTokenCreator" = var.owners } : {}
}

# Cloud Composer2 setup
resource "google_composer_environment" "env-fns-prod2" {
  name    = "env-fns-prod2"
  region  = var.region
  project = module.composer_project.project_id
  config {
    software_config {
      image_version  = var.composer2_image
      env_variables = {
        KAGGLE_USERNAME = var.kaggle_username
        KAGGLE_KEY = var.kaggle_key
       }
      pypi_packages = {
        apache-airflow-providers-google = ">=8.6.0"
        jsonschema = ""
        kaggle = ">=1.5.12"
        packaging = ""
        pyarrow = ">=9.0.0"
        google-cloud-storage= ">=2.6.0"
        airflow-dbt = ">=0.4.0"
        dbt-bigquery= ">=1.3.0"
      }
    }

    workloads_config {
      scheduler {
        cpu        = 2.0
        memory_gb  = 7.5
        storage_gb = 4.0
        count      = 1
      }
      web_server {
        cpu        = 2.0
        memory_gb  = 2.0
        storage_gb = 2.0
      }
      worker {
        cpu = 2.0
        memory_gb  = 7.5
        storage_gb = 4
        min_count  = 1
        max_count  = 3
      }

    }
    environment_size = "ENVIRONMENT_SIZE_MEDIUM"

    node_config {
      service_account = module.composer_sa.email
    }
  }
}