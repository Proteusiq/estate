# Advance Web Scraping
> Using dagster to schedule, monitor, and log. Postgres as backend and data storage. Celery[Redis] as a broker


![advance_image](images/advance_design.jpg)

> Migrating from airflow to dagster in progress ...

### Getting Started
Checklist to start our services

- [ ] make sure docker is running, and volume mounting is enabled.
- [ ] git clone [advance_scraping](https://github.com/Proteusiq/advance_scraping.git)
- [ ] set environment variables
- [ ] run the service with a single docker compose command

## Disclamer:
> Real estates data gathered through websites API are for personal use only. This project demonstrates how I built a pipeline to assist my family buy a house 🏡.
Where you to use that data beyond private use. Do contact estate websites API for permission.

### Git Clone
```bash
git clone https://github.com/Proteusiq/advance_scraping.git
cd advance_scraping
```


### Set Environment Variables

Set correct environment variables in all `*.env`. E.g. edit the `.env_demo` contents and save it as `.env`<br>
Each service has `service.env`. Edit them as you see fit.
> ⚠️ **WARNING**: Remember to add  `*.env` to .gitignore. Do not share your secrets. All `*.env` are left here for demonstration.

##### Check the environments to be set by docker-compose with:

```bash
docker compose config
```

Make sure you can see the environment variable docker-compose fetches from `.env`

See: [docker-compose](https://docs.docker.com/compose/reference/overview/) options.

<br>
<details><summary>Slack Integration</summary>
<p>

### How to Step-up Slack
Checklist to set slack integration

 - [ ] Create [Slack APP](https://api.slack.com/apps/) for a given channel
 - [ ] In `OAuth Tokens for Your Workspace` get the Bot User OAuth Token and set it in .env SLACK_TOKEN
 - [ ] Set `Bot Token Scopes` to `chat:write.customize`
 - [ ] Extra `Restrict API Token Usage` whitelist IP Address
 - [ ] In a given channel invite the bot e.g. `@BotName`

</p>
</details>

<br>

### Start services with a single command:

> **WARNING**: Postgres container has an issue with persisting data after a restart. Until then, we will use labelled volume
> Do `docker volume create --name=pgdata` to create a named volume (to delete `docker volume rm pgdata`)

```bash
docker compose up
```

Note: _Only the initial build_ will take a while. Go grab a cup of coffee as docker downloads and install the necessary tools. 
You can run the services in detach mode. `--detach` or `-d` flag. This will leave services running.

See: [docker-compose up](https://docs.docker.com/compose/reference/up/) options


#### Dagster UI
Head to `localhost:3000` on your browser. 

_Examples_: On Launchpad

<details>
  <summary>Jobs Config!</summary>

Running `make_service_job` requires config:
```yaml
ops:
  estate_service:
    config:
      url: "https://www.estate.dk/Services/PropertySearch/Search"
  nybolig_service:
    config:
      url: "https://www.nybolig.dk/Services/PropertySearch/Search"
resources:
  warehouse:
    config:
      table_name: services
```

example: `make_boliga_job` recent estates
```yaml
ops:
  get_boliga:
    config:
      start_page: 0
      end_page: 100
      pagesize: 800
      url: https://api.boliga.dk/api/v2/search/results
resources:
  warehouse:
    config:
      table_name: boliga_current
```

example: `make_boliga_job` sold estates
```yaml
ops:
  get_boliga:
    config:
      start_page: 0
      end_page: 100
      pagesize: 800
resources:
  warehouse:
    config:
      table_name: "boliga_sold"
```

</details>

<details>
  <summary>Tools</summary>

## UI Services:
![advance_image](images/airflow_dag.png)
- ##### Dagster:  `address:` __localhost:3000__ 
- ##### pgAdmin: `address:` __localhost:5050__ `default_email:` __pgadmin@example.com__ `default_pwd`: __admin__
- ##### minio: `address:` __localhost:9000__ `default_key:` __danpra__ `default_secret:` __miniopwd__


#### Postgres Admin Tool
Head to `localhost:5050`. Login with credentials used in your environment PGADMIN_DEFAULT_EMAIL and PGADMIN_DEFAULT_PASSWORD variables. Example: `danpra@example.com` and password `postgrespwd`

![postgres_image](images/pgAdmin_login.png)

Adding a connection to `postgres` DB in `pgAdmin`, click `Add New Server`. Type any name and select `Connection`. Name:Boliga > Host name/address: `postgres`: Postgres Username and Password and click `Save`
  
 ![postgres_image](images/pgAdmin_connection.png) 


# Dagster's Architecture:
[Coming soon]

</details>

### Stop services with:
Press `Ctrl + C` to stop our services without killing your volumes data. Then do

```bash
docker compose down
```

Use docker-compose down -v to remove also the volumes. 
> **WARNING**: remember to backup your data before removing volumes. 

```bash
docker compose down -v
```
See: [docker-compose down](https://docs.docker.com/compose/reference/down/) options


<details>
  <summary>OOP Rumbling</summary>

### Web Scraping and Design Pattern [Opinionated Rumbling]
A lazy programmer, like me, loves to write less yet comprehensive codes. (:) Yes, I said it). Design Pattern in Python is not as useful and, in most cases, an overkill as other languages like Java, C#, and C++. To design a simple bolig[_danish for estate_] scrapping tool from different estate websites in Denmark, I decided to use Abstract Factory Pattern.

Bolig (`webscrapers.abc_estates.Bolig`) ensures a single instance and single object that can be used by all other bolig related classes. The form of singleton design is Early Instantiation. We create an instance at load time. 

Bolig class also defines an interface[abstract class] for creating families of related objects without specifying their concrete sub-classes[functions]. This ensures consistency among all objects by isolating the client code from implementation. We want to use the same function but with different implementations. `get_page` and `get_pages` will always be called in the same way, but the implementation is different.

```python
# inheritance tree
Bolig                   # singleton and abstract
Boliga(Bolig)           # overides get_page and get_pages for boliga.dk api logic
Services(Bolig)         # overides get_page and get_pages for home.dk and estate.dk api logic
BoligaRecent(Boliga)    # initiate with recent boliga as url
BoligaSold(Boliga)      # initiate with sold boliga as url
Home(Services)          # initiate with home recent home as url
Estate(Services)        # initiate with estate recent home as url
```

</details>

### Todo:
- [X] Add a web-scraper examples
- [ ] Add simple Dagster examples
- [ ] Add custom error handling class

![dev](images/flow_example.png)


Docker Basics:

Kill all containers
```bash
docker container ps | awk {' print $1 '} | tail -n+2 > tmp.txt; for line in $(cat tmp.txt); do docker container kill $line; done; rm tmp.txt
```


# Dagster

Steps for local setup: assuming pyenv -v 2.3.0 and poetry 1.1.13 or above
```bash
pyenv install 3.10.4 && pyenv local 3.10.4
poetry install && poetry shell # or from scratch  `poetry add pandas httpx sqlalchemy psycopg2-binary dagster dagit`
```

## Dagster
```bash
# create a new dagster template with:
dagster new-project <project_name> 
```

## SQL

get tables
```sql
-- get all tables
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema='public';
```

Get the columns and tables
```sql
SELECT
	 table_name
    ,column_name
    ,data_type
FROM
	information_schema.columns
WHERE
	table_schema = 'public'
	-- AND table_name = 'home';
ORDER BY
    column_name ASC
```
