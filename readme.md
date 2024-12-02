### Weather info is fetched from yr.no portal, under Creative Commons Attribution 4.0 International Public License
### https://developer.yr.no/doc/License/
## Ingester
- install libraries 
```sh
 pip install requests pyyaml pytest kafka-python-ng schedule
```
- run ingester
```sh
python weather_ingester.py
```

## Kafka
- install Podman & podman-compose
```sh
podman machine init
podman machine start
pip install podman-compose
```
- run podman compose
```sh
podman-compose up -d
```
- setup kafka topic
```sh
podman exec -it kafka bash
kafka-topics.sh --create --topic weather --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
## Postgresql
- download and run postgresql on podman
```
podman pull docker.io/postgres:latest
podman run --name postgres -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=admin -p 5432:5432 -v /c/projects/postgress-podman -d postgres
```