#!/bin/sh
echo creating dashboard
curl --user admin:grafanapwd  -H 'Accept: application/json' -H 'Content-Type: application/json; charset=UTF-8' -X POST --data @datasource.js http://localhost:3000/api/datasources
echo datasource created