locals {
    envVersion = formatdate("hhmm",timestamp())
}

#device_name must be as below otherwise you get 2 ebs
resource "aws_launch_template" "main" {
  name = "${var.tags.Name}-lt"
  user_data = base64encode(data.template_file.user_data.rendered)
  tags = merge(var.tags, {Description = "${var.tags.Name} Compute Resources"})

  block_device_mappings {
    device_name = "/dev/xvda"
    no_device = "true"
    ebs {
      volume_size = 100
      volume_type = "gp3"
      encrypted = true
    }
  }
}

# Main compute environment used to run jobs
resource "aws_batch_compute_environment" "compenv_ec2" {
  compute_environment_name        = "${var.tags.Name}-EC2-${local.envVersion}"
  service_role                    = aws_iam_role.aws_batch_job_role.arn
  type                            = "MANAGED"

  compute_resources {
    # Compute Environment Size
    max_vcpus = 100
    min_vcpus = 0

    type = "EC2"

    image_id = var.ami
    instance_role = aws_iam_instance_profile.ecs_instance_role.arn
    instance_type = ["c5.large", "c5a.large", "m5a.large", "r5a.large", "r5.large"]

    # VPC Settings
    subnets = var.private_subnets
    security_group_ids = compact([
      aws_security_group.batch.id
    ])

    tags = merge(var.tags, {Description = "EC2 instance used in ${var.tags.Name} batch compute environment"})
    launch_template {
      launch_template_id = aws_launch_template.main.id
      version = aws_launch_template.main.latest_version
    }
  }
  lifecycle {
    create_before_destroy = true
  }
}

# Job queue
resource "aws_batch_job_queue" "compenv_ec2" {
  name                 = "${var.tags.Name}-EC2"
  state                = "ENABLED"
  priority             = 1
  compute_environments = [aws_batch_compute_environment.compenv_ec2.arn]
}

# Main compute environment used to run jobs
resource "aws_batch_compute_environment" "compenv_spot" {
  compute_environment_name        = "${var.tags.Name}-SPOT-${local.envVersion}"
  service_role                    = aws_iam_role.aws_batch_job_role.arn
  type                            = "MANAGED"

  compute_resources {
    # Compute Environment Size
    max_vcpus = 200
    min_vcpus = 0

    type = "SPOT"

    image_id = var.ami
    instance_role = aws_iam_instance_profile.ecs_instance_role.arn
    instance_type = ["c5.large", "c5a.large", "m5a.large", "r5a.large", "r5.large"]

    # VPC Settings
    subnets = var.private_subnets
    security_group_ids = compact([
      aws_security_group.batch.id
    ])

    spot_iam_fleet_role = aws_iam_role.amazon_ec2_spot_fleet_role[0].arn
    tags = merge(var.tags, {Description = "EC2 instance used in ${var.tags.Name} batch compute environment"})
    launch_template {
      launch_template_id = aws_launch_template.main.id
      version = aws_launch_template.main.latest_version
    }
  }
  lifecycle {
    create_before_destroy = true
  }
}

# Job queue
resource "aws_batch_job_queue" "compenv_spot" {
  name                 = "${var.tags.Name}-SPOT"
  state                = "ENABLED"
  priority             = 1
  compute_environments = [aws_batch_compute_environment.compenv_spot.arn]
}

# The following User Data object is inserted into the Launch Template.
# The var and local variables require this to be in the .tf file and not in a template.

data "template_file" "user_data" {

  template = <<EOF
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="==MYBOUNDARY=="

--==MYBOUNDARY==
Content-Type: text/cloud-config; charset="us-ascii"

packages:
  - cloud-utils-growpart
  - amazon-efs-utils
  - amazon-linux-extras

runcmd:

  - aws ec2 modify-volume --size 100
  - growpart /dev/xvda 1
  - resize2fs /dev/xvda1

  - echo "CREATING ECS CONFIG:"
  - sed s/ECS_DISABLE_IMAGE_CLEANUP\=false/ECS_DISABLE_IMAGE_CLEANUP\=true/ /etc/ecs/ecs.config > /tmp/ecs.config
  - echo "MOVING ECS CONFIG:"
  - mv /tmp/ecs.config /etc/ecs/ecs.config
  - echo "INSTALLING WGET:"
  - yum install -y -q wget
  - echo "DOWNLOADING CLOUDWATCH AGENT:"
  - wget https://s3.amazonaws.com/amazoncloudwatch-agent/amazon_linux/amd64/latest/amazon-cloudwatch-agent.rpm -P /tmp
  - echo "INSTALLING CLOUDWATCH AGENT:"
  - rpm -U /tmp/amazon-cloudwatch-agent.rpm
  - echo "CREATING CLOUDWATCH AGENT CONFIG FILE:"
  - echo '${jsonencode(
    {
      "metrics": {
        "namespace": "/${var.tags.Name}",
        "metrics_collected": {
          "cpu": {
            "measurement": [
              "cpu_usage_idle",
              "cpu_usage_iowait",
              "cpu_usage_user",
              "cpu_usage_system"
           ],
            "metrics_collection_interval": 60,
            "totalcpu": true
          },
          "disk": {
            "measurement": [
              "used_percent"
            ],
            "metrics_collection_interval": 60,
            "resources": [
              "*"
            ]
          },
          "diskio": {
            "measurement": [
              "io_time",
              "write_bytes",
              "read_bytes",
              "writes",
              "reads"
            ],
            "metrics_collection_interval": 60,
            "resources": [
              "*"
            ]
          },
          "mem": {
            "measurement": [
              "mem_used_percent"
            ],
            "metrics_collection_interval": 60
          }
        }
      }
    }
    )}' > /opt/aws/amazon-cloudwatch-agent/bin/config.json
  - echo "STARTING CLOUDWATCH AGENT:"
  - /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/bin/config.json

--==MYBOUNDARY==--
EOF
}
