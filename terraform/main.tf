terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}


resource "google_storage_bucket" "prj-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "prj_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

resource "google_service_account" "sa_mage_runner" {
  account_id   = "sa-mage-runner"
  display_name = "Service Account for Mage"
}


resource "google_project_iam_member" "access_mage_storage" {
  project = var.project
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.sa_mage_runner.email}"
}

resource "google_project_iam_member" "access_mage_bigquery" {
  project = var.project
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.sa_mage_runner.email}"
}

resource "google_project_iam_member" "access_mage_dataproc" {
  project = var.project
  role    = "roles/dataproc.admin"
  member  = "serviceAccount:${google_service_account.sa_mage_runner.email}"
}

resource "google_dataproc_cluster" "prj_dataproc" {

  project = var.project
  name    = "dpc-zoomcamp"
  region  = "europe-west1"

  cluster_config {
    staging_bucket = var.gcs_bucket_name

    master_config {
      num_instances = 1
      machine_type  = "e2-standard-4"
      disk_config {
        boot_disk_size_gb = 30
      }
    }

    worker_config {
      num_instances = 2
      machine_type  = "e2-standard-4"
      disk_config {
        boot_disk_size_gb = 30
      }
    }

    # software_config {
    #   override_properties = {
    #     "dataproc:dataproc.allow.zero.workers" = "true"
    #   }
    # }

  }
}