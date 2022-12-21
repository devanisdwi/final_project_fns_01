resource "random_string" "project_random_string" {
  length  = 7
  special = false
  upper   = false
}

# provider "google" {
#   project = var.project_id
#   region  = var.region
# }

module "composer_project" {
  source          = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/project"
  billing_account = var.billing_account_id
  prefix          = var.prefix
  name            = var.project_id
  iam_additive = {
    "roles/owner" = var.owners
  }
  # # Required for Cloud Composer
  # policy_boolean = {
  #   "constraints/compute.requireOsLogin" = false
  # }
  services        = [
    "composer.googleapis.com",
    "sourcerepo.googleapis.com",
    "bigquery.googleapis.com",
    "iamcredentials.googleapis.com",
    "pubsub.googleapis.com",
    "compute.googleapis.com"
  ]
}

module "composer_vpc" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/net-vpc"
  project_id = module.composer_project.project_id
  name       = "composer-vpc"
  subnets = [{
    ip_cidr_range      = "10.1.0.0/24"
    name               = "default"
    region             = var.region
    secondary_ip_range = {
      pods     = "10.16.0.0/14"
      services = "10.20.0.0/24"
    }
  }]
  # subnet_private_access = {
  #   "subnet" = true
  # }
}

data "google_project" "project" {}

module "composer_sa" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/iam-service-account"
  project_id = module.composer_project.project_id
  name       = "composer-sa"
  iam_additive = {
    "roles/composer.ServiceAgentV2Ext" = ["serviceAccount:service-${data.google_project.project.number}@cloudcomposer-accounts.iam.gserviceaccount.com"]
  }
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
      "roles/composer.admin",
      "roles/networkmanagement.admin",
      "roles/networkconnectivity.hubAdmin",
      "roles/composer.environmentAndStorageObjectAdmin",
      "roles/composer.sharedVpcAgent",
      "roles/composer.serviceAgent"
    ]
  }
  iam = var.owners != null ? { "roles/iam.serviceAccountTokenCreator" = var.owners } : {}
}

resource "google_project_iam_binding" "composer-sa-service-agentv2" {
    role    = "roles/composer.ServiceAgentV2Ext"
    project = var.project_id
     members = [
         "serviceAccount:service-${data.google_project.project.number}@cloudcomposer-accounts.iam.gserviceaccount.com"
     ]
}

resource "google_service_account_iam_member" "custom_service_account" {
  provider = google-beta
  service_account_id = "projects/${var.project_id}/serviceAccounts/composer-sa@${var.project_id}.iam.gserviceaccount.com"
  role = "roles/composer.ServiceAgentV2Ext"
  member = "serviceAccount:service-${data.google_project.project.number}@cloudcomposer-accounts.iam.gserviceaccount.com"
}

module "composer_firewall" {
  source       = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/net-vpc-firewall"
  project_id   = module.composer_project.project_id
  network      = module.composer_vpc.name
  # admin_ranges = [module.composer_vpc.subnet_ips["${var.region}/default"]]
}

module "composer_nat" {
  source         = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/net-cloudnat"
  project_id     = module.composer_project.project_id
  region         = var.region
  name           = "default"
  router_network = module.composer_vpc.self_link
}

# Cloud Composer2 setup
resource "google_composer_environment" "composer-fns-prod2" {
  name    = "composer-fns-prod2"
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
    # environment_size = "ENVIRONMENT_SIZE_MEDIUM"

    node_config {
      network         = module.composer_vpc.self_link
      subnetwork      = module.composer_vpc.subnet_self_links["${var.region}/default"]
      service_account = module.composer_sa.email
    }

    private_environment_config {
      enable_private_endpoint = true
    }
  }
}