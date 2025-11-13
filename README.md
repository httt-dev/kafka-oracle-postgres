https://debezium.io/releases/2.7/#installation

https://debezium.io/releases/3.1/

Linux:
```
    export DOCKER_HOST_IP=$(ifconfig | grep 'inet ' | grep -v 127.0.0.1 | awk '{print $2}')
```
Windows :
```bat
    $env:DOCKER_HOST_IP = (
        Get-NetIPConfiguration |
        Where-Object {
            $_.IPv4Address -ne $null -and
            $_.NetAdapter.Status -eq "Up" -and
            $_.NetAdapter.InterfaceDescription -notmatch "Virtual|Hyper|Docker|VMware|WSL|Loopback"
        } |
        Select-Object -First 1 -ExpandProperty IPv4Address |
        Select-Object -ExpandProperty IPv4Address
    )
    Write-Output $env:DOCKER_HOST_IP
```

## Xoa cac data trong folder data
Remove-Item -Recurse -Force .\data\


## Run docker compose
docker-compose up -d

## Stop docker compose
docker-compose down

## Get list of connectors

curl.exe -s http://localhost:8083/connector-plugins  

curl -s http://localhost:8083/connector-plugins

http://localhost:8083/connector-plugins


## connector 

### oracle connector
curl.exe -X POST http://localhost:8083/connectors   -H "Content-Type: application/json"   -d "@connectors\oracle-source-init-snapshot.json"
curl -X POST http://localhost:8083/connectors   -H "Content-Type: application/json"   -d @connectors\oracle-source-init-snapshot.json

curl.exe -X DELETE http://localhost:8083/connectors/oracle-source-init-snapshot

-- oracle-source-init-otp-snapshot
curl.exe -X POST http://localhost:8083/connectors   -H "Content-Type: application/json"   -d "@connectors\oracle-source-init-otp-snapshot.json"
curl.exe -X DELETE http://localhost:8083/connectors/oracle-source-init-otp-snapshot

### postgres connector
curl.exe -X POST http://localhost:8083/connectors  -H "Content-Type: application/json"   -d "@connectors\postgres-sink-init-snapshot.json"

curl.exe -X DELETE http://localhost:8083/connectors/postgres-sink-init-snapshot

curl.exe -X POST http://localhost:8083/connectors  -H "Content-Type: application/json"   -d "@connectors\postgres-sink-init-otp-snapshot.json"
curl.exe -X DELETE http://localhost:8083/connectors/postgres-sink-init-otp-snapshot


## Delete topics
kafka-topics.sh --bootstrap-server kafka1:9092 --list | grep -v '^__' | xargs -I {} kafka-topics
.sh --bootstrap-server kafka1:9092 --delete --topic {}

