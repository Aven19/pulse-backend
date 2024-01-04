# provider.tf

# Configure the AWS Provider
provider "aws" {
  shared_credentials_files = [var.credentials]
  region                   = var.aws_region
}
