resource "aws_cognito_user_pool" "user-pool" {
  name = "${local.app_env}"

  # username_attributes = ["email"]

  # mfa_configuration = "ON"

  # software_token_mfa_configuration {
  #   enabled = true
  # }

  account_recovery_setting {
    recovery_mechanism {
      name     = "verified_email"
      priority = 1
    }
  }

  tags = {
    Name        = "${local.app_env}-cognito-up"
    Environment = "${var.environment}"
  }

  # schema {
  #   attribute_data_type      = "String"
  #   developer_only_attribute = false
  #   mutable                  = true
  #   name                     = "email"
  #   required                 = true

  #   string_attribute_constraints {
  #     min_length = 1
  #     max_length = 256
  #   }
  # }

}

resource "aws_cognito_identity_provider" "google_provider" {
  user_pool_id  = aws_cognito_user_pool.user-pool.id
  provider_name = "Google"
  provider_type = "Google"

  provider_details = {
    authorize_scopes = "profile email openid"
    client_id        = var.google_client_id
    client_secret    = var.google_client_secret
  }

  attribute_mapping = {
    "email_verified" = "email_verified"
    "name"           = "name"
    "email"          = "email"
    "username"       = "sub"
  }

  depends_on = [
    aws_cognito_user_pool.user-pool
    ]
}

resource "aws_cognito_identity_provider" "amazon_provider" {
  user_pool_id  = aws_cognito_user_pool.user-pool.id
  provider_name = "LoginWithAmazon"
  provider_type = "LoginWithAmazon"

  provider_details = {
    authorize_scopes = "profile profile:user_id postal_code"
    client_id        = var.amazon_client_id
    client_secret    = var.amazon_client_secret
  }

  attribute_mapping = {
    "email"         = "email"
    "username"      = "user_id"
    "name"          = "name"
  }

    depends_on = [
      aws_cognito_user_pool.user-pool
    ]

}

resource "aws_cognito_user_pool_client" "app-client" {
  name                          = "${local.app_env}-app-client"
  user_pool_id                  = aws_cognito_user_pool.user-pool.id
  generate_secret               = false
  supported_identity_providers  = ["COGNITO", "Google", "LoginWithAmazon"]
  explicit_auth_flows           = ["ALLOW_REFRESH_TOKEN_AUTH", "ALLOW_USER_SRP_AUTH", "ALLOW_USER_PASSWORD_AUTH", "ALLOW_ADMIN_USER_PASSWORD_AUTH", "ALLOW_CUSTOM_AUTH"]
  prevent_user_existence_errors = "LEGACY"

  allowed_oauth_flows = ["implicit"]
  allowed_oauth_scopes = [
    "aws.cognito.signin.user.admin",
    "email",
    "openid",
    "phone",
    "profile",
  ]

  access_token_validity = 10080
  id_token_validity = 60
  refresh_token_validity = 30

  token_validity_units {
    access_token = "seconds"
    id_token = "minutes"
    refresh_token = "days"
  }

  callback_urls = [var.cognito_callback_urls]

  logout_urls = [var.cognito_logout_urls]

}

# resource "aws_cognito_user_pool_domain" "main" {
#   domain = var.frontend_endpoint
#   certificate_arn = data.aws_acm_certificate.frontend_certificate.arn
#   user_pool_id    = aws_cognito_user_pool.user-pool.id
# }
