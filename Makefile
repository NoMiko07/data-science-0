# Define the Docker Compose file
COMPOSE_FILE=docker-compose.yml

# Detect the operating system
ifeq ($(OS),Windows_NT)
    # Windows specific commands
    DOCKER_COMPOSE = docker-compose
    RM = del
    OS_TYPE = Windows
else
    # Linux/macOS specific commands
    DOCKER_COMPOSE = docker compose
    RM = rm -rf
    OS_TYPE = $(shell uname -s)
endif

# Command to start containers (detached mode)
up:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d

# Command to stop and remove containers, volumes, and networks
down:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down -v

# Command to clean everything and restart
restart: down up

# Command to view logs of the running containers
logs:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) logs -f

# Command to view running containers
ps:
	docker ps

# Command to clean up Docker (⚠️ Deletes all containers and volumes)
clean: down
	docker system prune -a --volumes -f

# Command to open a bash shell inside the PostgreSQL container
exec:
	docker exec -it db_piscineds bash

# Command to display the environment variables in the PostgreSQL container
env:
	docker exec -it db_piscineds env



# Help command to display available Make commands
help:
	@echo "Operating System: $(OS_TYPE)"
	@echo "Available commands:"
	@echo "  make up       -> Start the containers"
	@echo "  make down     -> Stop and remove containers and volumes"
	@echo "  make restart  -> Restart containers cleanly"
	@echo "  make logs     -> View logs of running containers"
	@echo "  make ps       -> View currently running containers"
	@echo "  make clean    -> Clean up (⚠️ including images and volumes)"
