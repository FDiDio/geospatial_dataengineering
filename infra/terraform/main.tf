locals {
  services = [
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "cloudscheduler.googleapis.com",
    "iam.googleapis.com",
    "run.googleapis.com",
    "storage.googleapis.com",
  ]
  image = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.pipeline.repository_id}/era5-pipeline:${var.image_tag}"
}

resource "google_project_service" "services" {
  for_each           = toset(local.services)
  service            = each.value
  disable_on_destroy = false
}

# Processed data: the Hive-partitioned Parquet datasets written by the pipeline
resource "google_storage_bucket" "lake" {
  name                        = var.lake_bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  depends_on                  = [google_project_service.services]
}

resource "google_artifact_registry_repository" "pipeline" {
  repository_id = "era5"
  location      = var.region
  format        = "DOCKER"
  description   = "Images of the ERA5 pipeline"

  cleanup_policies {
    id     = "keep-recent-versions"
    action = "KEEP"
    most_recent_versions {
      keep_count = 10
    }
  }

  depends_on = [google_project_service.services]
}

# --- Ingestion: a Cloud Run job writing to the lake bucket, mounted with Cloud Storage FUSE ---

resource "google_service_account" "pipeline" {
  account_id   = "era5-pipeline"
  display_name = "ERA5 ingestion job"
}

resource "google_storage_bucket_iam_member" "pipeline_writes_lake" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.pipeline.member
}

resource "google_cloud_run_v2_job" "ingestion" {
  name                = "era5-ingestion"
  location            = var.region
  deletion_protection = false

  template {
    task_count = 1

    template {
      service_account       = google_service_account.pipeline.email
      timeout               = "3600s"
      max_retries           = 1
      execution_environment = "EXECUTION_ENVIRONMENT_GEN2"

      containers {
        image = local.image
        # ERA5 is published ~6 days after real time; days already written are skipped,
        # so each daily execution also catches up on the days a previous one missed
        args = concat(
          ["--start_date", "today-12", "--end_date", "today-6", "--workers", tostring(var.workers), "--skip_queries"],
          ["--variables"], var.variables,
        )

        resources {
          limits = {
            cpu    = var.job_cpu
            memory = var.job_memory
          }
        }

        volume_mounts {
          name       = "lake"
          mount_path = "/app/output"
        }
      }

      volumes {
        name = "lake"
        gcs {
          bucket    = google_storage_bucket.lake.name
          read_only = false
        }
      }
    }
  }

  depends_on = [google_project_service.services, google_storage_bucket_iam_member.pipeline_writes_lake]
}

# --- Daily trigger ---

resource "google_service_account" "scheduler" {
  account_id   = "era5-scheduler"
  display_name = "Triggers the ERA5 ingestion job"
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_runs_ingestion" {
  name     = google_cloud_run_v2_job.ingestion.name
  location = google_cloud_run_v2_job.ingestion.location
  role     = "roles/run.invoker"
  member   = google_service_account.scheduler.member
}

resource "google_cloud_scheduler_job" "daily_ingestion" {
  name             = "era5-daily-ingestion"
  region           = var.region
  schedule         = var.schedule
  time_zone        = "Etc/UTC"
  attempt_deadline = "320s"

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/${google_cloud_run_v2_job.ingestion.id}:run"

    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }

  depends_on = [google_cloud_run_v2_job_iam_member.scheduler_runs_ingestion]
}

# --- BigQuery: the Parquet datasets as external tables, partitioned by year and month ---

resource "google_bigquery_dataset" "era5" {
  dataset_id  = "era5"
  location    = var.region
  description = "Hourly ERA5 variables indexed with H3, read in place from the lake bucket."
  depends_on  = [google_project_service.services]
}

resource "google_bigquery_table" "hourly" {
  for_each            = toset(var.variables)
  dataset_id          = google_bigquery_dataset.era5.dataset_id
  table_id            = "hourly_${each.value}"
  deletion_protection = false

  external_data_configuration {
    source_format = "PARQUET"
    autodetect    = true
    # *.parquet skips the hidden temporary files of the writes in progress
    source_uris = ["gs://${google_storage_bucket.lake.name}/${each.value}/*.parquet"]

    hive_partitioning_options {
      mode                     = "AUTO"
      source_uri_prefix        = "gs://${google_storage_bucket.lake.name}/${each.value}/"
      require_partition_filter = true
    }
  }
}
