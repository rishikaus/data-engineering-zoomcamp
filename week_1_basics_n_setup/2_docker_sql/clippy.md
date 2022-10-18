# start a postgres container
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v /Users/rishi/gitwork/data-engineering-zoomcamp/week_1_basics_n_setup/ny_pgData:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres:13

# start a networked postgres container
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v /Users/rishi/gitwork/data-engineering-zoomcamp/week_1_basics_n_setup/ny_pgData:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:13

# start pgadmin connector on same network
docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    --network=pg-network \
    --name pgadmin \
    dpage/pgadmin4

# connect to the postgres container
pgcli -h localhost -p 5432 -u root -d ny_taxi

# zoomcamp help links
https://github.com/ziritrion/dataeng-zoomcamp/blob/main/notes/1_intro.md
https://github.com/mharty3/data_engineering_zoomcamp_2022
