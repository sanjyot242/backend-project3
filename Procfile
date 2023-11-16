
enrollment_service: uvicorn enrollment_service.enrollment_service:app --port $PORT --reload
login_service_primary: ./bin/litefs mount -config etc/primary.yml
login_secondary: ./bin/litefs mount -config etc/secondary.yml
login_tertiary: ./bin/litefs mount -config etc/tertiary.yml
worker: echo ./etc/krakend.json | entr -nrz krakend run --config etc/krakend.json --port $PORT
dynamoDB: cd bin/dynamodb_local_latest && java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb -port $PORT
