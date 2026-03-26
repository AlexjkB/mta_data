variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "mta-data-491403"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-east1"
}

variable "gcs_bucket_name" {
  description = "GCS bucket for raw data"
  type        = string
  default     = "mta-data-491403-bucket"
}

variable "credentials_file" {
  description = "Path to GCP service account JSON key"
  type        = string
}

variable "bq_dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "mta_warehouse"
}
