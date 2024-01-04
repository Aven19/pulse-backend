resource "aws_key_pair" "access_key" {
  key_name   = var.key_name
  public_key = var.public_key
}

resource "aws_instance" "redis_server" {
  ami           = var.ec2_ami_id
  instance_type = var.ec2_instance_class
  associate_public_ip_address = true
  disable_api_termination = true
  key_name = aws_key_pair.access_key.id
  vpc_security_group_ids = [aws_security_group.ecomm_pulse_redis_server.id]
  subnet_id = aws_subnet.public[0].id

  tags = {
    Name        = "${local.app_env}-redis-server"
  }

  user_data = <<-EOL
  #!/bin/bash -xe
  sudo apt-get update
  sudo apt -y install redis-server
  sed -i'' -e 's_bind 127.0.0.1_bind 0.0.0.0_g' /etc/redis/redis.conf
  sed -i'' -e 's_protected-mode yes_protected-mode no_g' /etc/redis/redis.conf
  sudo service redis-server restart
  EOL
}

resource "aws_eip" "redis_server_ip" {
  instance = aws_instance.redis_server.id
  vpc      = true
}
