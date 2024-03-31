## Requirements

No requirements.

## Providers

| Name | Version |
|------|---------|
| <a name="provider_google"></a> [google](#provider\_google) | 5.22.0 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [google_bigquery_dataset.dataset](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset) | resource |
| [google_bigquery_table.table](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_table) | resource |
| [google_service_account.bqowner](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/service_account) | resource |
| [google_storage_bucket.data-lake](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket) | resource |
| [google_storage_bucket.temp-files](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_data-lake-bucket"></a> [data-lake-bucket](#input\_data-lake-bucket) | Bucket being used as data lake. | `any` | n/a | yes |
| <a name="input_dataset-name"></a> [dataset-name](#input\_dataset-name) | Bucket being used as data lake. | `any` | n/a | yes |
| <a name="input_project_id"></a> [project\_id](#input\_project\_id) | GCP project id. | `any` | n/a | yes |
| <a name="input_region"></a> [region](#input\_region) | GCP project default region. | `any` | n/a | yes |
| <a name="input_table-name"></a> [table-name](#input\_table-name) | BigQuery table used to store data. | `any` | n/a | yes |
| <a name="input_temp-file-bucket"></a> [temp-file-bucket](#input\_temp-file-bucket) | Bucket needed to store temporary files which are generated while running dataflow jobs. | `any` | n/a | yes |

## Outputs

No outputs.
