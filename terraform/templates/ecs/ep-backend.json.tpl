[
  {
    "name": "${local_app_env}",
    "image": "${app_image}",
    "cpu": ${fargate_cpu},
    "memory": ${fargate_memory},
    "networkMode": "awsvpc",
    "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/${local_app_env}",
          "awslogs-region": "${aws_region}",
          "awslogs-stream-prefix": "ecs"
        }
    },
    "portMappings": [
      {
        "containerPort": ${app_port},
        "hostPort": ${app_port}
      },
      {
        "containerPort": ${rds_db_port},
        "hostPort": ${rds_db_port}
      },
      {
        "containerPort": ${smtp_port},
        "hostPort": ${smtp_port}
      }
    ]
  }
]
