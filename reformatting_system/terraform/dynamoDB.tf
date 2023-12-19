# this dynamodb table tracks batch jobs status
resource "aws_dynamodb_table" "dynamodb" {
  name = "job-tracking"
  billing_mode = "PAY_PER_REQUEST"
  stream_enabled = true
  stream_view_type = "NEW_IMAGE"
  hash_key = "job-date" # i.e. webscrape-20230317
  range_key = "jobID"
  attribute {
    name = "job-date"
    type = "S"
  }
  attribute {
    name = "jobID"
    type = "S"
  }
  point_in_time_recovery {
      enabled = true
  }
  tags = var.tags
}

# this dynamodb table tracks batch jobs status
resource "aws_dynamodb_table" "dynamodb2" {
  name = "gnss-ro-data-stagingv2_0"
  billing_mode = "PAY_PER_REQUEST"
  stream_enabled = true
  stream_view_type = "NEW_IMAGE"
  hash_key = "leo-ttt" # i.e. sacc-G03
  range_key = "date-time"  #i.e. 2006-03-09-15-51
  attribute {
    name = "leo-ttt"
    type = "S"
  }
  attribute {
    name = "date-time"
    type = "S"
  }
  point_in_time_recovery {
      enabled = true
  }
  tags = var.tags
}

