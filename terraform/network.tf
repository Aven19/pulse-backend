# network.tf

# Fetch AZs in the current region
data "aws_availability_zones" "available" {
}

/*====
The VPC
======*/

resource "aws_vpc" "ep-backend" {
  cidr_block           = "172.17.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name        = "${local.app_env}-vpc"
    Environment = "${var.environment}"
  }
}

/*====
Subnets
======*/
/* Internet gateway for the public subnet */
resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.ep-backend.id

  tags = {
    Name        = "${local.app_env}-igw"
    Environment = "${var.environment}"
  }
}

/* Elastic IP for NAT */
resource "aws_eip" "gw" {
  count      = var.az_count //length of private subnets
  vpc        = true
  depends_on = [aws_internet_gateway.igw]

  tags = {
    Name        = "${local.app_env}-gw"
    Environment = "${var.environment}"
  }
}

/* NAT */
resource "aws_nat_gateway" "ngw" {
  count         = var.az_count //length of private subnets
  allocation_id = element(aws_eip.gw.*.id, count.index)
  subnet_id     = element(aws_subnet.public.*.id, count.index)
  depends_on    = [aws_internet_gateway.igw]

  tags = {
    Name        = "${local.app_env}-ngw"
    Environment = "${var.environment}"
  }
}

/* Public subnet */
resource "aws_subnet" "public" {
  count                   = var.az_count
  cidr_block              = cidrsubnet(aws_vpc.ep-backend.cidr_block, 8, var.az_count + count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  vpc_id                  = aws_vpc.ep-backend.id
  map_public_ip_on_launch = true

  tags = {
    Name        = "${local.app_env}-public-sub"
    Environment = "${var.environment}"
  }
}

/* Private subnet */
resource "aws_subnet" "private" {
  count                   = var.az_count
  cidr_block              = cidrsubnet(aws_vpc.ep-backend.cidr_block, 8, count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  vpc_id                  = aws_vpc.ep-backend.id
  map_public_ip_on_launch = false

  tags = {
    Name        = "${local.app_env}-private-sub"
    Environment = "${var.environment}"
  }
}

/* Routing table for public subnet */
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.ep-backend.id

  tags = {
    Name        = "${local.app_env}-rt-public"
    Environment = "${var.environment}"
  }
}

resource "aws_route" "public" {
  count                  = var.az_count #Length of public subnet
  route_table_id         = element(aws_route_table.public.*.id, count.index)
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = element(aws_internet_gateway.igw.*.id, count.index)
}

resource "aws_route_table_association" "public" {
  count          = var.az_count //length of public subnets
  subnet_id      = element(aws_subnet.public.*.id, count.index)
  route_table_id = element(aws_route_table.public.*.id, count.index)
}

resource "aws_route_table" "private" {
  count  = var.az_count #Length of private subnet
  vpc_id = aws_vpc.ep-backend.id

  tags = {
    Name        = "${local.app_env}-rt-pivate"
    Environment = "${var.environment}"
  }
}

resource "aws_route" "private" {
  count                  = var.az_count #Length of private subnet
  route_table_id         = element(aws_route_table.private.*.id, count.index)
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = element(aws_nat_gateway.ngw.*.id, count.index)
}

resource "aws_route_table_association" "private" {
  count          = var.az_count
  subnet_id      = element(aws_subnet.private.*.id, count.index)
  route_table_id = element(aws_route_table.private.*.id, count.index)
}

/*====
RDS
======*/

/* subnet used by rds */
resource "aws_db_subnet_group" "ep-backend-rds-subnet-group" {
  name        = "${local.app_env}-rds-sb-group"
  description = "${local.app_env} RDS subnet group"
  subnet_ids  = flatten([aws_subnet.private.*.id])

  tags = {
    Name        = "${local.app_env}-rds-sb-group"
    Environment = "${var.environment}"
  }
}
