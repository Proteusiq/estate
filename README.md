# Advance Web Scraping
> Using airflow to schedule, monitor, and log. Postgres to store schedule and scrapped data. Python to do the magic



## Repos that made this project possible:


- [Docker Apache Airflow](https://github.com/puckel/docker-airflow)

Kill all containers

```bash
docker container ps | awk {' print $1 '} | tail -n+2 > tmp.txt; for line in $(cat tmp.txt); do docker container kill $line; done; rm tmp.txt
```
