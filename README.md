# Fashia Backend API

FastAPI-based backend application for the Fashia project.

## Features

- FastAPI REST API
- SQLite database integration
- SQLAlchemy ORM
- CORS enabled
- Health check endpoints

## Prerequisites

- Python 3.8+
- pip
- SQLite database file (facilities.db)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/flutterflowdevs/fashia-backend-api
cd fashia-backend-api
```

2. Create a virtual environment:
```bash
python3 -m venv .venv
```

3. Activate the virtual environment:
```bash
source .venv/bin/activate  # On macOS/Linux
# or
.venv\Scripts\activate  # On Windows
```

4. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

The application expects a SQLite database at:
```
/Users/flutterflowdevs/Desktop/continue_execution/fashia-data-importer-oct-11-mac-final/facilities.db
```

You can modify the database path in `app/db/session.py` if needed.

### Environment Variables (Optional)

Create a `.env` file in the root directory for custom configuration:
```
DATABASE_URL=postgresql://user:password@localhost:5432/fashia
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```

## Running the Application

### Development Mode (with auto-reload)

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Production Mode

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

The API will be available at: `http://localhost:8000`

## API Endpoints

### Health Check
- **GET** `/api/health` - Check API health status

### Hello
- **GET** `/api/hello` - Returns a hello message

### Entities
- **GET** `/entities/count` - Get count of entities in database

## API Documentation

Once the server is running, you can access:
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

## Docker Deployment (AWS)

### Build and Push to AWS ECR

```bash
# Login to AWS ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 050752622202.dkr.ecr.us-east-1.amazonaws.com

# Build the image
docker build -t fashia-backend-api .

# Tag the image
docker tag fashia-backend-api:latest 050752622202.dkr.ecr.us-east-1.amazonaws.com/fashia-backend-api:latest

# Push to ECR
docker push 050752622202.dkr.ecr.us-east-1.amazonaws.com/fashia-backend-api:latest
```

### Run with Docker

```bash
docker build -t fashia-backend-api .
docker run -p 8000:8000 fashia-backend-api
```

## Project Structure

```
fashia-backend-api/
├── app/
│   ├── config.py          # Application settings
│   ├── main.py            # FastAPI application entry point
│   ├── controllers/       # API route handlers
│   ├── db/                # Database configuration
│   ├── models/            # SQLAlchemy models
│   └── services/          # Business logic services
├── requirements.txt       # Python dependencies
├── Dockerfile            # Docker configuration
└── README.md            # This file
```

## License

MIT



