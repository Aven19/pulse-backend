# alb.tf

resource "aws_alb" "main" {
  name            = "${local.app_env}-alb"
  subnets         = aws_subnet.public.*.id
  security_groups = [aws_security_group.lb.id]

  tags = {
    Name        = "${local.app_env}-alb"
    Environment = "${var.environment}"
  }
}

resource "aws_alb_target_group" "app" {
  name        = "${local.app_env}-alb-tg"
  port        = 80
  protocol    = "HTTP"
  vpc_id      = aws_vpc.ep-backend.id
  target_type = "ip"

  lifecycle {
    create_before_destroy = true
  }

  health_check {
    healthy_threshold   = "3"
    interval            = "30"
    protocol            = "HTTP"
    matcher             = "200"
    timeout             = "3"
    path                = var.health_check_path
    unhealthy_threshold = "2"
  }

  tags = {
    Name        = "${local.app_env}-alb-tg"
    Environment = "${var.environment}"
  }
}
# Get ACM Certificate
data "aws_acm_certificate" "issued" {
  domain = var.server_endpoint
  statuses = ["ISSUED"]
}

# data "aws_acm_certificate" "frontend_certificate" {
#   domain   = var.frontend_endpoint
#   statuses = ["ISSUED"]
# }

# Redirect all traffic from the ALB to the target group
resource "aws_alb_listener" "https" {
  load_balancer_arn = aws_alb.main.id
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-2016-08"
  certificate_arn   = data.aws_acm_certificate.issued.arn

  default_action {
    target_group_arn = aws_alb_target_group.app.id
    type             = "forward"
  }

  tags = {
    Name        = "${local.app_env}-alb-https"
    Environment = "${var.environment}"
  }
}


# Redirect from port 80 to 443.
resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_alb.main.id
  port              = "80"
  protocol          = "HTTP"

  default_action {
    type = "redirect"

    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }

  tags = {
    Name        = "${local.app_env}-alb-http"
    Environment = "${var.environment}"
  }
}
