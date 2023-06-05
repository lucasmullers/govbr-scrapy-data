echo Removendo o código atual dentro da pasta DAGs do Airflow local
rm -rf ./docker/dags/

echo Copiando o source para a pasta DAGs do Airflow local
cp --recursive ./dags/ ./docker/dags/