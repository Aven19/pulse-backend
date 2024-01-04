# logs.tf

# Set up CloudWatch group and log stream and retain logs for 30 days
resource "aws_cloudwatch_log_group" "ep-backend_log_group" {
  name              = "/ecs/${local.app_env}"
  retention_in_days = 30

  tags = {
    Name        = "${local.app_env}-cw-log-group"
    Environment = "${var.environment}"
  }
}

resource "aws_cloudwatch_log_stream" "ep-backend_log_stream" {
  name           = "${local.app_env}-log-stream"
  log_group_name = aws_cloudwatch_log_group.ep-backend_log_group.name
}
