output "lake_bucket" {
  description = "Bucket holding the processed data."
  value       = google_storage_bucket.lake.url
}

output "image" {
  description = "Image run by the ingestion job: build and push it before the first execution."
  value       = local.image
}

output "ingestion_job" {
  description = "Run it on demand with: gcloud run jobs execute <name> --region <region>"
  value       = google_cloud_run_v2_job.ingestion.name
}

output "bigquery_tables" {
  description = "External tables over the Parquet datasets."
  value       = { for name, table in google_bigquery_table.hourly : name => "${var.project_id}.${table.dataset_id}.${table.table_id}" }
}
