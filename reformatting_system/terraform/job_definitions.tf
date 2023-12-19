module "ro-processing-framework2" {
  source = "./jobdef_component"
  name = "ro-processing-framework-test"
  ecr_url = "${aws_ecr_repository.ecr_repository_framework.repository_url}:test"
  batch_role_arn = aws_iam_role.aws_batch_job_role.arn
}

module "ro-processing-framework" {
  source = "./jobdef_component"
  name = "ro-processing-framework"
  ecr_url = "${aws_ecr_repository.ecr_repository_framework.repository_url}"
  batch_role_arn = aws_iam_role.aws_batch_job_role.arn
}
