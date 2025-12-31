# Infrastructure

This folder contains Terraform IaC to provision cloud resources for the data platform. The local environment uses Docker (Postgres, MinIO). The Terraform stack targets AWS for production-like deployment.

## AWS Stack (Terraform)
- S3 bucket for data lake with versioning and SSE
- (Optional) RDS Postgres for warehouse (not included by default)

### Usage
1. Install Terraform and set AWS credentials in your environment.
2. Update variables in `terraform/aws/terraform.tfvars` (or pass via CLI).
3. Init & apply:
   - `cd infra/terraform/aws`
   - `terraform init`
   - `terraform apply -var bucket_name=<your-unique-bucket> -var region=us-east-1`

Notes:
- Bucket names must be globally unique.
- This is a minimal starting point; integrate with your VPC/IAM as needed.
