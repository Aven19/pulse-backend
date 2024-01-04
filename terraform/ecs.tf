# ecs.tf

resource "aws_ecs_cluster" "main" {
  name = "${local.app_env}-ecs-cluster"

  tags = {
    Name        = "${local.app_env}-ecs-cluster"
    Environment = "${var.environment}"
  }
}

data "template_file" "ep_backend_template" {
  template = file("./templates/ecs/ep-backend.json.tpl")

  vars = {
    smtp_port      = var.smtp_port
    rds_db_port    = var.rds_db_port
    env            = var.environment
    app_image      = "${aws_ecr_repository.ecr_repo.repository_url}:latest"
    app_port       = var.app_port
    fargate_cpu    = var.fargate_cpu
    fargate_memory = var.fargate_memory
    aws_region     = var.aws_region
    local_app_env  = local.app_env
  }
}

resource "aws_ecs_task_definition" "app" {
  family                   = "${local.app_env}-app-task"
  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_execution_role.arn
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.fargate_cpu
  memory                   = var.fargate_memory
  container_definitions    = data.template_file.ep_backend_template.rendered

   tags = {
    Name        = "${local.app_env}-ecs-task-definition"
    Environment = "${var.environment}"
  }
}

resource "aws_ecs_service" "main" {
  name                   = "${local.app_env}-ecs-service"
  cluster                = aws_ecs_cluster.main.id
  task_definition        = aws_ecs_task_definition.app.arn
  desired_count          = var.app_count
  launch_type            = "FARGATE"
  enable_execute_command = true

  network_configuration {
    security_groups  = [aws_security_group.ecs_tasks.id]
    subnets          = aws_subnet.private.*.id
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_alb_target_group.app.arn
    container_name   = local.app_env
    container_port   = var.app_port
  }

  depends_on = [aws_iam_role_policy_attachment.ecs_task_execution_role]

  # lifecycle {
  #   ignore_changes = [task_definition, desired_count]
  # }

  tags = {
    Name        = "${local.app_env}-ecs-service"
    Environment = "${var.environment}"
  }
}
