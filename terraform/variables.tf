variable "project" {
  description = "Project ID"
  default     = "biomonitorcapstone"
}

variable "region" {
  description = "Region for GCP resources"
  default     = "europe-west1"
}

variable "location" {
  description = "Project location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "BigQuery dataset name"
  default     = "biomonitor_data"
}

variable "gcs_bucket_name" {
  description = "torage Bucket name"
  default     = "biomonitor-data-lake-DEcapstone"
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}

variable "credentials" {
  description = "Key credentials"
  default     = "../google_credentials.json"
}
