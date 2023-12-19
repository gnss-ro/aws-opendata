##### ECS #####
resource "aws_iam_role" "ecs_instance_role" {
  name = "${var.tags.Name}-ECSinstanceRole"
  path = "/"
  tags = merge(var.tags, {Description = "${var.tags.Name} ECS Instance Role"})
  assume_role_policy = data.aws_iam_policy_document.ecs_policy.json
}

data "aws_iam_policy_document" "ecs_policy" {
  statement {
    sid        = "AssumeRole"
    actions    = ["sts:AssumeRole"]
    effect     = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy_attachment" "ecs_instance_role" {
  role       = aws_iam_role.ecs_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_role_policy_attachment" "cloudwatch_agent_policy_attachment" {
  role       = aws_iam_role.ecs_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

resource "aws_iam_role_policy" "ecs_access_policy" {
  name    = "${var.tags.Name}-Access-Policy"
  role    = aws_iam_role.ecs_instance_role.id

  policy  = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
            "s3:ListBucket",
            "s3:GetObject*",
            "s3:DeleteObject",
            "s3:PutObject",
            "s3:PutObjectACL"
          ],
          "Resource": "*"
      },
      {
        "Effect": "Allow",
        "Action": [
          "dynamodb:PutItem",
          "dynamodb:DescribeTable",
          "dynamodb:ListTables",
          "dynamodb:DeleteItem",
          "dynamodb:GetItem",
          "dynamodb:GetRecords",
          "dynamodb:Scan",
          "dynamodb:Query",
          "dynamodb:UpdateItem",
          "dynamodb:UpdateTable",
          "dynamodb:ListStreams",
          "dynamodb:DescribeStream",
          "dynamodb:GetShardIterator",
          "dynamodb:BatchGetItem",
          "dynamodb:ExportTableToPointInTime",
          "dynamodb:DescribeExport"
        ],
        "Resource": "*"
      },
      {
          "Effect": "Allow",
          "Action": "batch:SubmitJob",
          "Resource": "*"
      }
    ]
  }
  EOF
}

resource "aws_iam_instance_profile" "ecs_instance_role" {
  name = "${var.tags.Name}-instance-profile"  # this is the instance role for the compute env.  But it's not an IAM role.
  path = "/${var.tags.Name}/"
  role = aws_iam_role.ecs_instance_role.name
}

##### BATCH #####
resource "aws_iam_role" "aws_batch_job_role" {
  name = "${var.tags.Name}-BatchRole"
  path = "/${var.tags.Name}/"
  tags = merge(var.tags, {Description = "${var.tags.Name} Batch Job Role"})
  assume_role_policy = data.aws_iam_policy_document.batch_job_policy.json
}

data "aws_iam_policy_document" "batch_job_policy" {
  statement {
    sid        = "AssumeRole"
    actions    = ["sts:AssumeRole"]
    effect     = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com","batchoperations.s3.amazonaws.com","batch.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "batch_ecs_policy" {
  name    = "${var.tags.Name}-BatchECS-Policy"
  role    = aws_iam_role.aws_batch_job_role.id

  policy  = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
            "ecs:ListClusters",
            "ecs:DeleteCluster"
        ],
        "Resource": "*"
      }
    ]
  }
  EOF
}

resource "aws_iam_role_policy_attachment" "aws_batch_job_role_policy" {
  role       = aws_iam_role.aws_batch_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}

resource "aws_iam_role_policy_attachment" "ecs_instance_role_batch" {
  role       = aws_iam_role.aws_batch_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

##### LAMBDA #####
# This role allows lambda functions to use the AWS SDK to call out to
# other AWS services to perform API operations
resource "aws_iam_role" "lambda_role" {
  name = "${var.tags.Name}-LambdaRole"
  tags = var.tags
  path = "/"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "lambda_api_role_vpc_policy_attachment" {
  role = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy_attachment" "aws_xray_write_only_access" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSXrayWriteOnlyAccess"
}

resource "aws_iam_role_policy" "lambda_s3_access_policy" {
  name    = "${var.tags.Name}-S3Access-Policy"
  role    = aws_iam_role.lambda_role.id

  policy  = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
            "s3:List*",
            "s3:Get*",
            "s3:Put*"
        ],
        "Resource": "*"
      },
      {
        "Effect": "Allow",
        "Action": [
          "batch:DescribeJobQueues",
          "batch:DescribeJobs",
          "batch:SubmitJob",
          "batch:ListJobs"
        ],
        "Resource": "*"
      },
      {
        "Effect": "Allow",
        "Action": [
          "dynamodb:PutItem",
          "dynamodb:DescribeTable",
          "dynamodb:ListTables",
          "dynamodb:DeleteItem",
          "dynamodb:GetItem",
          "dynamodb:GetRecords",
          "dynamodb:Scan",
          "dynamodb:Query",
          "dynamodb:UpdateItem",
          "dynamodb:UpdateTable",
          "dynamodb:ListStreams",
          "dynamodb:DescribeStream",
          "dynamodb:GetShardIterator",
          "dynamodb:BatchGetItem"
        ],
        "Resource": "*"
      },
      {
          "Effect": "Allow",
          "Action": "sns:Publish",
          "Resource": "*"
      }
    ]
  }
  EOF
}

#
# SPOT Roles & Policies
#

resource "aws_iam_role_policy_attachment" "fleet_instance_role" {
  count = 1
  role       = aws_iam_role.ecs_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole"
}


resource "aws_iam_role_policy_attachment" "aws_batch_fleet_role" {
  role       = aws_iam_role.aws_batch_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole"
}

#
# Spot Fleet Role for batch compute environment
#
# A managed compute environment that uses Amazon EC2 Spot Fleet Instances requires
# a role that grants the Spot Fleet permission to launch, tag, and terminate instances.
resource "aws_iam_role" "amazon_ec2_spot_fleet_role" {
  count = 1

  name = "${var.tags.Name}-ec2-spot-fleet-role"
  path = "/${var.tags.Name}/"
  tags = merge(var.tags, {Description = "${var.tags.Name} Spot Fleet Role"})

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "spotfleet.amazonaws.com"
      },
      "Effect": "Allow"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "amazon_ec2_spot_fleet_role_policy_attachment" {
  count = 1
  role = aws_iam_role.amazon_ec2_spot_fleet_role[0].name
  # This role provides all of the necessary permissions to tag Amazon EC2 Spot Instances.
  # It replaced AmazonEC2SpotFleetRole
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole"
}

resource "aws_iam_role_policy_attachment" "cloudwatch_agent_fleet_policy_attachment" {
  count = 1
  role       = aws_iam_role.amazon_ec2_spot_fleet_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}
