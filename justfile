sim_time := "$(awk -F '=' '/simulation_time_seconds/ {print $2}' config.ini)"

default:
    just -l

start:
    docker-compose up