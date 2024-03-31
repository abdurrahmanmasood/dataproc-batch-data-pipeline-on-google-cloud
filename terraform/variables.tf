variable "project_id" {
  description = "GCP project id."
}

variable "region" {
  description = "GCP project region."
}

variable "data-lake-bucket" {
  description = "Bucket used as data lake."
}

variable "temp-file-bucket" {
  description = "Bucket need to store temporary files which are generated while running dataflow job."
}

variable "dataset-name" {
  description = "BigQuery dataset used to store table."
}

variable "table-name" {
  description = "BigQuery table used to store data."
}

variable "dataproc-cluster-name" {
  description = "Dataproc cluster name."
}

variable "template_gcs_path" {
  description = "Dataproc job template location."
}
