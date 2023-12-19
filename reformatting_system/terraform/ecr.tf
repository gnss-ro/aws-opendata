# ECR Repositories used to store Docker images
resource "aws_ecr_repository" "ecr_repository_framework" {
  name                 = "ro-processing-framework"
  image_tag_mutability = "MUTABLE"
  encryption_configuration {
    encryption_type = "AES256"
  }
  tags = merge(var.tags, { Description = "${var.tags.Name} Container Registry" })
}
resource "aws_ecr_lifecycle_policy" "ecr_lifepolicy_framework" {
  repository = aws_ecr_repository.ecr_repository_framework.name
  policy = <<EOF
{
    "rules": [
        {
            "rulePriority": 1,
            "description": "Keep last 5 images",
            "selection": {
                "tagStatus": "untagged",
                "countType": "imageCountMoreThan",
                "countNumber": 5
            },
            "action": {
                "type": "expire"
            }
        }
    ]
}
EOF
}
