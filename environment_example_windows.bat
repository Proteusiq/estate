:: Set up environment variables to control building and running Airflow
:: > CMD.exe environment_example_windows.bat


@ECHO OFF 
:: This batch file sets the environment variables in Windows systems.
TITLE Set Docker-Compose Variables
ECHO Date            Time          
ECHO %date%     %time%
ECHO Hold on... Checking docker information.
:: Section 1: Docker information.
ECHO Docker INFO
docker info | findstr /c:"Server Version"



:: Section 2: Set Environments.
ECHO ==========================================================================
ECHO Setting Airflow Envs


SET ADMIN_USER=danpra
SET ADMIN_PASSWORD=airflowpwd
SET DEFAULT_TIMEZONE=Europe/Copenhagen
SET FERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho=

SET POSTGRES_USER=danpra
SET POSTGRES_PASSWORD=postgrespwd
SET BOLIG_DB=bolig_db

SET PGADMIN_DEFAULT_EMAIL=danpra@example.com
SET PGADMIN_DEFAULT_PASSWORD=postgrespwd

ECHO ==========================================================================
ECHO Task Complete: All varibles set. What are you waiting for! Fire that App up 


:: Comment below to disalbe the script to run the serives.
:: Section 3: Run the service.
ECHO ==========================================================================
ECHO Starting Services in the background
REM If already up try to take it down before start
docker-compose down  
docker-compose up -d --build

REM To follow the log do docker-compose logs -f and Ctrl+C to get out


