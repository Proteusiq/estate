# Design

Structure Inspired by Danny Janz's `Containerize your whole Data Science Environment (or anything you want) with Docker-Compose`. This is a collection of different github repos modified to fit my needs.

## Repos that made this project possible:


- [Docker Apache Airflow](https://github.com/puckel/docker-airflow)

Kill all containers

```bash
docker container ps | awk {' print $1 '} | tail -n+2 > tmp.txt; for line in $(cat tmp.txt); do docker container kill $line; done; rm tmp.txt
```