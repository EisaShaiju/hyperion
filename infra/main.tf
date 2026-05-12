terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# This reads the AWS_REGION from your .env file eventually
provider "aws" {
  region = "ap-south-1" 
}

# S3 Data Lake (Phase 2 & Phase 8)
resource "aws_s3_bucket" "hyperion_data_lake" {
  bucket = "hyperion-data-lake-dev" # S3 bucket names must be globally unique
}

resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.hyperion_data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}