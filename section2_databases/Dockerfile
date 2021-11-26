FROM postgres
ENV POSTGRES_PASSWORD docker
ENV POSTGRES_DB car_sales
COPY car_sales_db_init.sql /docker-entrypoint-initdb.d/ 
