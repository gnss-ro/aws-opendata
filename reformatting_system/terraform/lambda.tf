#builds dependent python libraries into zip archive of lambda (used when not using .gitlab-ci.yml)
resource "null_resource" "ucar-pip" {
  triggers = {
    requirements = "${base64sha256(file("lambda/ucarWebScrapeToS3/requirements.txt"))}"
  }

  provisioner "local-exec" {
    command = "pip3 install -r lambda/ucarWebScrapeToS3/requirements.txt -t lambda/ucarWebScrapeToS3/lib"
  }
}

data "archive_file" "ucar_webscrape_zip" {
  type = "zip"
  source_dir = "./lambda/ucarWebScrapeToS3/"
  output_path = "./.tmp/lambda/ucarWebScrapeToS3/main.zip"

  depends_on = [null_resource.ucar-pip]
}

#python3.8 as Amy's local pip3 is python3.8
resource "aws_lambda_function" "ucar_webscrape" {
  filename = data.archive_file.ucar_webscrape_zip.output_path
  source_code_hash = data.archive_file.ucar_webscrape_zip.output_base64sha256
  description = "Setup ucar webscrape lambda"
  function_name = "ucar-webscrape"
  role = aws_iam_role.lambda_role.arn
  handler = "main.lambda_handler"
  runtime = "python3.8"
  memory_size = "1000"
  timeout = "900"

  depends_on = [null_resource.ucar-pip]
  tags = var.tags
}

resource "null_resource" "romsaf-pip" {
  triggers = {
    requirements = "${base64sha256(file("lambda/romsafWebScrapeToS3/requirements.txt"))}"
  }

  provisioner "local-exec" {
    command = "pip3 install -r lambda/romsafWebScrapeToS3/requirements.txt -t lambda/romsafWebScrapeToS3/lib"
  }
}

data "archive_file" "romsaf_webscrape_zip" {
  type = "zip"
  source_dir = "./lambda/romsafWebScrapeToS3/"
  output_path = "./.tmp/lambda/romsafWebScrapeToS3/main.zip"

  depends_on = [null_resource.romsaf-pip]
}

#python3.8 as Amy's local pip3 is python3.8
resource "aws_lambda_function" "romsaf_webscrape" {
  filename = data.archive_file.romsaf_webscrape_zip.output_path
  source_code_hash = data.archive_file.romsaf_webscrape_zip.output_base64sha256
  description = "Setup romsaf webscrape lambda"
  function_name = "romsaf-webscrape"
  role = aws_iam_role.lambda_role.arn
  handler = "main.lambda_handler"
  runtime = "python3.8"
  memory_size = "1000"
  timeout = "900"

  depends_on = [null_resource.romsaf-pip]
  tags = var.tags
}
