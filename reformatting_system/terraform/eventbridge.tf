#crons run at noon utc on the 15, 16, 10 of each month respectively
resource "aws_cloudwatch_event_rule" "lambda-trigger-rule" {
  name = "trigger-ucar-webscrape-first"
  description = "Trigger lambda per schedule_expression"
  schedule_expression = "cron(0 7 14 * ? *)"
}
resource "aws_cloudwatch_event_target" "lambda-target" {
  arn = aws_lambda_function.ucar_webscrape.arn
  rule = aws_cloudwatch_event_rule.lambda-trigger-rule.name
}


resource "aws_cloudwatch_event_rule" "lambda-trigger-rule2" {
  name = "trigger-ucar-webscrape-second"
  description = "Trigger lambda per schedule_expression"
  schedule_expression = "cron(0 7 15 * ? *)"
}
resource "aws_cloudwatch_event_target" "lambda-target2" {
  arn = aws_lambda_function.ucar_webscrape.arn
  rule = aws_cloudwatch_event_rule.lambda-trigger-rule2.name
}

resource "aws_cloudwatch_event_rule" "lambda-trigger-rule3" {
  name = "trigger-export-and-sync"
  description = "Trigger lambda per schedule_expression"
  schedule_expression = "cron(0 15 16 * ? *)"
}
resource "aws_cloudwatch_event_target" "lambda-target3" {
  arn = aws_lambda_function.ucar_webscrape.arn
  rule = aws_cloudwatch_event_rule.lambda-trigger-rule3.name
}

resource "aws_cloudwatch_event_rule" "lambda-trigger-rule-romsaf" {
  name = "trigger-romsaf-webscrape"
  description = "Trigger lambda per schedule_expression"
  schedule_expression = "cron(0 7 15 * ? *)"
}

resource "aws_cloudwatch_event_target" "lambda-target-romsaf" {
  arn = aws_lambda_function.romsaf_webscrape.arn
  rule = aws_cloudwatch_event_rule.lambda-trigger-rule-romsaf.name
}

resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id = "AllowExecutionFromCloudWatch"
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ucar_webscrape.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.lambda-trigger-rule.arn
}

resource "aws_lambda_permission" "allow_cloudwatch2" {
  statement_id = "AllowExecutionFromCloudWatch2"
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ucar_webscrape.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.lambda-trigger-rule2.arn
}

resource "aws_lambda_permission" "allow_cloudwatch3" {
  statement_id = "AllowExecutionFromCloudWatch3"
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ucar_webscrape.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.lambda-trigger-rule3.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_romsaf" {
  statement_id = "AllowExecutionFromCloudWatch"
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.romsaf_webscrape.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.lambda-trigger-rule-romsaf.arn
}
