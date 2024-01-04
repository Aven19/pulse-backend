# outputs.tf

output "app_env" {
  value = local.app_env
}

output "vpc_id" {
  description = "The ID of the VPC"
  value       = concat(aws_vpc.ep-backend.*.id, [""])[0]
}

output "rds_endpoint" {
  value = aws_db_instance.ep-backend-database.address
}

output "alb_hostname" {
  value = aws_alb.main.dns_name
}

output "cognito_pool_id" {
  value = aws_cognito_user_pool.user-pool.id
}

output "cognito_app_client_id" {
  value = aws_cognito_user_pool_client.app-client.id
}

output "aws_ecr_repository" {
  value = aws_ecr_repository.ecr_repo
}

output "s3_bucket_domain_name" {
  value = aws_s3_bucket.config-bucket.bucket_domain_name
}

output "S3_BUCKET_URL" {
  value = aws_s3_bucket.config-bucket.bucket_domain_name
}

output "redis_server_ip" {
  value = aws_instance.redis_server.private_ip
}
