{
  "cpu": 2048,
  "environment": [
    {
      "name": "SQLITE_DB_PATH",
      "value": "/tmp/fashia.db"
    },
    {
      "name": "EFS_DB_PATH",
      "value": "/mnt/efs/fashia-db/fashia.db"
    }
  ],
  "environmentFiles": [],
  "essential": true,
  "image": "050752622202.dkr.ecr.us-east-1.amazonaws.com/fashia-backend-api@sha256:36f6c2992af65536840662997b30a27cdbc15e7a204bb0eb0461d9b711a89c60",
  "logConfiguration": {
    "logDriver": "awslogs",
    "options": {
      "awslogs-group": "/ecs/fashia-backend-task",
      "awslogs-create-group": "true",
      "awslogs-region": "us-east-1",
      "awslogs-stream-prefix": "ecs"
    },
    "secretOptions": []
  },
  "memory": 7168,
  "memoryReservation": 4096,
  "mountPoints": [
    {
      "containerPath": "/mnt/efs",
      "readOnly": false,
      "sourceVolume": "efs-storage"
    }
  ],
  "name": "fashia-task-container",
  "portMappings": [
    {
      "appProtocol": "http",
      "containerPort": 80,
      "hostPort": 80,
      "name": "fashia-task-container-80-tcp",
      "protocol": "tcp"
    }
  ],
  "systemControls": [],
  "ulimits": [],
  "volumesFrom": []
}
