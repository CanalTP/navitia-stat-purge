# navitia-stat-purge

Purge script to remove old data from navitia statistics database.

## Usage

psql -v retention=<retention_days_#> -f script_purge.sql -U<postgresql_user> -h<postgresql_server> statistics
