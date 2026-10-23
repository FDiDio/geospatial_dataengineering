variable "project_id" {
  description = "GCP project hosting the lakehouse."
  type        = string
}

variable "region" {
  description = "Region of every resource. The public ERA5 bucket is in us-central1: reading it from the same region has no network egress cost."
  type        = string
  default     = "us-central1"
}

variable "lake_bucket_name" {
  description = "Name of the bucket holding the processed data (globally unique)."
  type        = string
}

variable "image_tag" {
  description = "Tag of the pipeline image pushed to the Artifact Registry repository."
  type        = string
  default     = "latest"
}

variable "variables" {
  description = "ERA5 variables processed by the scheduled job, and exposed as BigQuery tables."
  type        = list(string)
  default     = ["total_precipitation", "2m_temperature"]
}

variable "schedule" {
  description = "Cron schedule of the ingestion job (UTC)."
  type        = string
  default     = "0 6 * * *"
}

variable "workers" {
  description = "Variable-days processed in parallel by a job execution (~1.5 GB of memory each)."
  type        = number
  default     = 4
}

variable "job_cpu" {
  description = "vCPUs of the ingestion job."
  type        = string
  default     = "4"
}

variable "job_memory" {
  description = "Memory of the ingestion job."
  type        = string
  default     = "16Gi"
}
