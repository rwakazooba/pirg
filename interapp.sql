CREATE TABLE districts(
    id SERIAL PRIMARY KEY NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    dhis2id TEXT NOT NULL DEFAULT '',
    cdate TIMESTAMP DEFAULT NOW()
);

CREATE TABLE facilities(
    id SERIAL PRIMARY KEY NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    dhis2id TEXT NOT NULL DEFAULT '',
    uuid TEXT NOT NULL DEFAULT '',
    cdate TIMESTAMP DEFAULT NOW()
);

\copy districts(name, dhis2id) FROM '~/projects/unicef/interapp/districts.csv' WITH DELIMITER ',' CSV HEADER;
