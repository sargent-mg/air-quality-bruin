terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials_file)
}

# ---------- Variables ----------

variable "project_id" {
  description = "GCP project ID"
  type        = string
  
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "US"
}

variable "credentials_file" {
  description = "Path to service account key file"
  type        = string
  
}

# ---------- GCS Bucket (raw data landing) ----------

resource "google_storage_bucket" "raw_data" {
  name          = "${var.project_id}-air-quality-raw"
  location      = var.region
  force_destroy = true
  storage_class = "STANDARD"

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  versioning {
    enabled = true
  }
}

# ---------- BigQuery Datasets ----------

resource "google_bigquery_dataset" "raw" {
  dataset_id    = "air_quality_raw"
  friendly_name = "Air Quality - Raw"
  description   = "Raw ingested data from OpenAQ"
  location      = var.region

  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "staging" {
  dataset_id    = "air_quality_staging"
  friendly_name = "Air Quality - Staging"
  description   = "Cleaned and standardized data"
  location      = var.region

  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "marts" {
  dataset_id    = "air_quality_marts"
  friendly_name = "Air Quality - Marts"
  description   = "Analytics-ready tables"
  location      = var.region

  delete_contents_on_destroy = true
}

output "gcs_bucket_name" {
  value = google_storage_bucket.raw_data.name
}

output "bq_raw_dataset" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "bq_staging_dataset" {
  value = google_bigquery_dataset.staging.dataset_id
}

output "bq_marts_dataset" {
  value = google_bigquery_dataset.marts.dataset_id
}