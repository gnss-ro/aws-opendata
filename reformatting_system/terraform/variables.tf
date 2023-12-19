# Name and Project Tags
variable "tags" {
  description = "Tags to be used by all resources"
  type        = object({
    Name    = string
    Project = string
  })
}

variable "prod" {
  type        = bool
  description = "Flag for Prod/NonProd"
}

variable "aws_profile" {
  description = "AWS profile to use for credentials"
  default     = ""
  type        = string
}

variable "ami" {
  description = "ecs optimized aws linux ami for batch"
  default     = ""
  type        = string
}

# AWS Configuration
variable "aws_region" {
  type        = string
  description = "AWS Region"
}

variable "vpc_id" {
  type        = string
  description = "AWS VPC"
}

variable "aws_account_id" {
  type        = string
  description = "AWS account number"
}

variable "private_subnets" {
  type        = list(string)
  description = "Private AWS Subnets for batch jobs"
}

variable "subnet_cidr" {
  description = "Subnet CDIR for batch"
  type = string
}

variable "subnet_cidrProd" {
  description = "Subnet CDIR of the workspace subnet in prod"
  type = string
}
