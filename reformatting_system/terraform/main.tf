# Configure state backend
terraform {
  backend "http" {
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.41.0"
    }
  }
}



# Configure AWS Provider
provider "aws" {
  region                  = var.aws_region
  profile                 = var.aws_profile == "" ? null : var.aws_profile
  skip_metadata_api_check = true
}
