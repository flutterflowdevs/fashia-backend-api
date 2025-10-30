# fashia-backend-api

# fashia-backend-api

# Aws Cmd
    - aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 050752622202.dkr.ecr.us-east-1.amazonaws.com
    - git clone https://github.com/flutterflowdevs/fashia-backend-api
    - cd fashia-backend-api
    - docker build -t fashia-backend-api .
    - docker tag fashia-backend-api:latest 050752622202.dkr.ecr.us-east-1.amazonaws.com/fashia-backend-api:latest
    - docker push 050752622202.dkr.ecr.us-east-1.amazonaws.com/fashia-backend-api:latest



