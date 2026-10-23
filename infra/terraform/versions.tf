terraform {
  required_version = ">= 1.9"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.0"
    }
  }

  # Keep the state in a GCS bucket created beforehand:
  # terraform init -backend-config="bucket=<state-bucket>"
  backend "gcs" {
    prefix = "era5-lakehouse"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
