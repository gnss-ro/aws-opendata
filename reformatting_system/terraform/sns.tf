resource "aws_sns_topic" "webscrape_notifications" {
  name = "webscrape"
  kms_master_key_id = "alias/aws/sns"
  tags = var.tags
}
