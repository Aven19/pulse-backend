# Terraform

Terraform is an open-source infrastructure as code software tool created by HashiCorp. It allows you to define and manage your infrastructure as code, making it easier to automate and scale your deployments.

Terraform uses a declarative language called HashiCorp Configuration Language (HCL) to define your infrastructure. With Terraform, you can define your infrastructure resources, such as servers, databases, and load balancers, in a single configuration file. You can then use Terraform to create, modify, and delete those resources as needed.

## Usage

Typically, the base Terraform will only need to be run once, and then should only
need changes very infrequently. After the base is built, each environment can be built.

```
# Move into the base directory
$ cd base

# This command initializes a new or existing Terraform working directory, downloading the required providers and modules.
$ terraform init

# This command generates an execution plan, showing which actions Terraform will take when you apply your configuration.
$ terraform plan

# This command checks your Terraform configuration for syntax errors and validates the configuration against the provider schema.
$ terraform validate

# This command applies the changes defined in your configuration to the infrastructure, creating, modifying, or deleting resources as needed.
$ terraform apply

# This command allows you to view and modify the state file, which contains the current state of your infrastructure.
$ terraform state

# This command displays the outputs defined in your configuration.
$ terraform output
```

##### Important (after initial `terraform apply`)

The generated base `.tfstate` is not stored in the remote state S3 bucket. Ensure the base `.tfstate` is checked into your infrastructure repo.
