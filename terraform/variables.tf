# variables.tf

variable "app_name" {
  description = "Application Name"
  default     = ""
}

variable "environment" {
  description = "Deployment Server Environment Name: prod/staging/dev"
  default     = ""
}

variable "aws_region" {
  description = "The AWS region things are created in"
  default     = ""
}

# variable "cidr" {
#   description = "The CIDR block for the VPC"
#   type        = string
#   default     = ""
# }

variable "health_check_path" {
  description = "Path to check if the service is healthy, e.g. \"/status\""
  default     = ""
}

variable "app_port" {
  description = "Port exposed by the docker image to redirect traffic to"
  default     = ""
}

variable "smtp_port" {
  description = "Port exposed by the docker image to deliver mail"
  default     = ""
}

variable "rds_instance_class" {
  description = "RDS Instance type"
  default     = ""
}

variable "rds_db_name" {
  description = "Database Name for RDS Instance"
  default     = ""
}

variable "rds_username" {
  description = "Username for RDS Instance"
  default     = ""
}

variable "rds_password" {
  description = "Password for RDS Instance"
  default     = ""
}

variable "app_count" {
  description = "Number of docker containers to run"
  default     = ""
}

variable "rds_db_port" {
  description = "RDS port"
  default     = ""
}

variable "az_count" {
  description = "Number of AZs to cover in a given region"
  default     = ""
}

variable "fargate_cpu" {
  description = "Fargate instance CPU units to provision (1 vCPU = 1024 CPU units)"
  default     = ""
}

variable "fargate_memory" {
  description = "Fargate instance memory to provision (in MiB)"
  default     = ""
}

variable "google_client_id" {
  description = "Client ID for Google identity provider in AWS Cognito"
  default     = ""
}

variable "google_client_secret" {
  description = "Client secret for Google identity provider in AWS Cognito"
  default     = ""
}

variable "amazon_client_id" {
  description = "Client ID for Amazon identity provider in AWS Cognito"
  default     = ""
}

variable "amazon_client_secret" {
  description = "Client secret for Amazon identity provider in AWS Cognito"
  default     = ""
}

variable "cognito_callback_urls" {
  description = "List of callback URLs for authentication in AWS Cognito"
  default     = ""
}

variable "cognito_logout_urls" {
  description = "List of logout URLs for sign-out in AWS Cognito"
  default     = ""
}

variable "key_name" {
  description = "Access Key Name"
  default     = ""
}

variable "public_key" {
  description = "Public Key of System"
  default     = ""
}

variable "credentials" {
  description = "Credential File Location"
}

variable "server_endpoint" {
  description = "Server Endpoint On which we will be receiving the api requests."
}

# variable "frontend_endpoint" {
#   description = "The endpoint where the web application is hosted on the front end"
# }

variable "ec2_instance_class" {
  description = "Instance Class for Redis Server"
  default     = ""
}

variable "ec2_ami_id" {
  description = "AMI Id for the instance"
  default     = ""
}
