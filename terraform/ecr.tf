resource "aws_ecr_repository" "ecr_repo" {
  name = "${local.app_env}"

  encryption_configuration {
    encryption_type = "AES256"
  }

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name        = "${local.app_env}-ecr-repo"
    Environment = "${var.environment}"
  }
}

resource "aws_ecr_lifecycle_policy" "main" {
  repository = aws_ecr_repository.ecr_repo.name

  policy = jsonencode({
   rules = [{
     rulePriority = 1
     description  = "keep last 10 images"
     action       = {
       type = "expire"
     }
     selection     = {
       tagStatus   = "any"
       countType   = "imageCountMoreThan"
       countNumber = 10
     }
   }]
  })
}
