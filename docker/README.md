# Docker Setup for Torro Data Discovery Platform

## Quick Start

### 1. Create Environment File

Create `.env` file in `docker/` directory:

```env
MYSQL_ROOT_PASSWORD=your_secure_password
AIRFLOW_FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
```

### 2. Build and Start

```bash
cd docker
docker-compose up -d
```

### 3. Initialize Airflow Database

```bash
docker exec -it torro-airflow-webserver airflow db init
docker exec -it torro-airflow-webserver airflow users create \
  --username airflow \
  --firstname Airflow \
  --lastname User \
  --role Admin \
  --email airflow@example.com \
  --password airflow
```

### 4. Access Services

- **Frontend**: http://localhost:5162
- **Backend API**: http://localhost:8099
- **Airflow UI**: http://localhost:8080
- **MySQL**: localhost:3306

## Services

- **mysql**: MySQL 8.0 database
- **backend**: Flask API server
- **airflow-webserver**: Airflow web UI
- **airflow-scheduler**: Airflow scheduler
- **frontend**: React frontend (Nginx)

## Commands

### Start Services
```bash
docker-compose up -d
```

### Stop Services
```bash
docker-compose down
```

### View Logs
```bash
docker-compose logs -f [service_name]
```

### Rebuild Services
```bash
docker-compose build --no-cache
docker-compose up -d
```

### Access Container Shell
```bash
docker exec -it torro-backend bash
docker exec -it torro-airflow-webserver bash
```

## Configuration

Update environment variables in `docker-compose.yml` or use `.env` file.

## Troubleshooting

### Database Connection Issues
```bash
docker exec -it torro-mysql mysql -uroot -p
```

### Airflow Not Starting
```bash
docker exec -it torro-airflow-webserver airflow db init
```

### Rebuild After Code Changes
```bash
docker-compose build --no-cache backend
docker-compose up -d backend
```

