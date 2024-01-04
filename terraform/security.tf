# security.tf

# ALB Security Group: Edit to restrict access to the application
resource "aws_security_group" "lb" {
  name = "${local.app_env}-lb-sg"
  # description = "controls access to the ALB"
  description = "Allow HTTP from Anywhere into ALB"

  vpc_id = aws_vpc.ep-backend.id

  ingress {
    protocol         = "tcp"
    from_port        = 443
    to_port          = 443
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  # Allow all outbound traffic.
  egress {
    protocol         = "-1"
    from_port        = 0
    to_port          = 0
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Name        = "${local.app_env}-lb-sg"
    Environment = "${var.environment}"
  }
}

# Traffic to the ECS cluster should only come from the ALB
resource "aws_security_group" "ecs_tasks" {
  name        = "${local.app_env}-ecs-tasks-sg"
  description = "allow inbound access from the ALB only"
  vpc_id      = aws_vpc.ep-backend.id

  ingress {
    protocol         = "tcp"
    from_port        = var.app_port
    to_port          = var.app_port
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  # ingress {
  #   protocol         = "tcp"
  #   from_port        = 443
  #   to_port          = 443
  #   cidr_blocks      = ["0.0.0.0/0"]
  #   ipv6_cidr_blocks = ["::/0"]
  # }

  egress {
    protocol         = "-1"
    from_port        = 0
    to_port          = 0
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Name        = "${local.app_env}-sg-ecs-tasks"
    Environment = "${var.environment}"
  }
}

/* Security Group for resources that want to access the Database */
resource "aws_security_group" "ep-backend-db-access-sg" {
  vpc_id      = aws_vpc.ep-backend.id
  name        = "${local.app_env}-db-access-sg"
  description = "Allow access to RDS"

  tags = {
    Name        = "${local.app_env}-sg-db-access"
    Environment = "${var.environment}"
  }
}

resource "aws_security_group" "ep-backend-rds-sg" {
  name        = "${local.app_env}-rds-sg"
  description = "${local.app_env} Security Group"
  vpc_id      = aws_vpc.ep-backend.id

  // allows traffic from the SG itself
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }

  //allow traffic for TCP 5432
  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = ["${aws_security_group.ep-backend-db-access-sg.id}"]
  }

  // allow traffic from private subnets
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = aws_subnet.private.*.cidr_block
  }

  // outbound internet access
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${local.app_env}-sg-rds"
    Environment = "${var.environment}"
  }
}


# Security Group
resource "aws_security_group" "ecomm_pulse_redis_server" {
  name = "${local.app_env}-redis-ec2-sg"
  description = "Redis Server Security Group (terraform-managed)"
  vpc_id = aws_vpc.ep-backend.id

  ingress {
    from_port = 22
    to_port = 22
    protocol = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Redis port
  ingress {
    from_port = 6379
    to_port = 6379
    protocol = "tcp"
    cidr_blocks = [aws_vpc.ep-backend.cidr_block]
  }

  # Allow all outbound traffic.
  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  lifecycle {
      create_before_destroy = true
  }
}
