variable "composer2_image" {
  description = "Composer2 image to be pulled"
  type = string
  default = "composer-2.1.2-airflow-2.3.4"
}

variable kaggle_username {
  description = "Kaggle API Username from the json file"
  type = string
  default = "<YOUR_KAGGLE_USERNAME>"
}

variable kaggle_key {
  description = "Kaggle API Key from the json file"
  type = string
  default = "<YOUR_KAGGLE_KEY>"
}

variable "region" {
  description = "The region for resources and networking"
  type = string
  default = "us-west2"
}

variable "bq_dataset" {
  description = "BigQuery dataset"
  type        = string
  default     = "final_project_data"
}

variable "project_id" {
  description = "Project ID for the GCP"
  type = string
  default = "final-prod-g1212"
}

variable "organization" {
  description = "Organization for the project/resources"
  type = string
  default = "No organization"
}

variable "prefix" {
  description = "Prefix used to generate project id and name."
  type        = string
  default     = null
}

variable "root_node" {
  description = "Parent folder or organization in 'folders/folder_id' or 'organizations/org_id' format."
  type        = string
  default     = "IYKRA/iykra01"
}

variable "billing_account_id" {
  description = "Billing account for the projects/resources"
  type = string
  default = "13243435"
}

variable "owners" {
  description = "List of owners for the projects and folders"
  type = list(string)
  default = ["user:admin@example.com"]
}

variable "quota_project" {
  description = "Quota project used for admin settings"
  type = string
  default = "final-t1-quota"
}