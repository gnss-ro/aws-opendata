tags = {
  Name = "ro-processing"
  Project = "NASA ACCESS19 GNSSRO"
}

prod = false

aws_profile = "default"

#Amazon ECS-Optimized Amazon Linux 2023 (AL2023) x86_64 AMI-59df75d1-d8e6-4875-ba3b-cb9e651751c6
ami = "ami-01c14e50374bf9b34"

aws_region = "us-east-1"
vpc_id = "vpc-???????"

aws_account_id = "<AwsAccountNumber>"

private_subnets = [
  "subnet-?????", #us-east-1a
]

subnet_cidr = "100.99.999.0/20"
subnet_cidrProd = "100.99.999.999/26"
