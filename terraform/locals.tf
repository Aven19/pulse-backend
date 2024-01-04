locals {
  app_env = "${lower(replace(var.app_name, " ", "-"))}-${var.environment}"
}
