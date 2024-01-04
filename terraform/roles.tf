# ECS task execution role data
data "aws_iam_policy_document" "ecs_task_execution_role" {
  version = "2012-10-17"
  statement {
    sid     = ""
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# ECS task execution role
resource "aws_iam_role" "ecs_task_execution_role" {
  name               = "${local.app_env}-TaskExecutionRole"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_execution_role.json
}

resource "aws_iam_policy" "policy" {
  name        = "${local.app_env}-TaskExecutionPolicy"
  path        = "/ep-backend-${var.environment}/"
  description = "Task Execution Policy for ep-backend"

  policy = jsonencode(
    {
      Version : "2012-10-17",
      Statement : [
        {
          Action : [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "ecr:GetAuthorizationToken",
            "ecr:BatchCheckLayerAvailability",
            "ecr:BatchGetImage",
            "ecr:CompleteLayerUpload",
            "ecr:GetDownloadUrlForLayer",
            "ecr:GetLifecyclePolicy",
            "ecr:InitiateLayerUpload",
            "ecr:PutImage",
            "ecr:UploadLayerPart",
          ],
          Resource : "*",
          Effect : "Allow"
        },
        {
          Action : [
            "s3:PutObject",
            "s3:GetObject",
            "s3:ListObject",
            "s3:ListBucket"
          ],
          Resource : "*",
          Effect : "Allow"
        },
        {
          Effect : "Allow",
          Action : [
            "ssmmessages:CreateControlChannel",
            "ssmmessages:CreateDataChannel",
            "ssmmessages:OpenControlChannel",
            "ssmmessages:OpenDataChannel"
          ],
          Resource : "*"
        },
        {
          Effect : "Allow",
          Action : [
            "rds:Describe*"
          ],
          # Resource : [
          #   aws_db_instance.ep-backend-database.arn
          # ]
          Resource : "*"
        },
        {
          Effect : "Allow",
          Action : ["cognito-idp:*"],
          # Resource : [aws_cognito_user_pool.user-pool.arn]
          Resource : "*"
        }
      ]
    }
  )
}

# ECS task execution role policy attachment
resource "aws_iam_role_policy_attachment" "ecs_task_execution_role" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = aws_iam_policy.policy.arn
}
