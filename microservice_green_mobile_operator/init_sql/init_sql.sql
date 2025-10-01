CREATE SCHEMA IF NOT EXISTS hp;

DROP TABLE IF EXISTS hp.characters;

CREATE TABLE hp.characters
(

    ID       serial PRIMARY KEY,
    name   text
);