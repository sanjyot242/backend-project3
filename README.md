# CPSC 449 Project 3
Create a new user authentication service with read replication, then use it to implement authentication and load balancing through an API gateway.

## Installation
run `sh ./bin/install.sh`.

## How to run
- run `sh run.sh` to start the services
- run `sh ./bin/create-user-db.sh` to create user database
- run `python3 ./enrollment_service/database/create_dynamo_table.py` to create enrollment service database