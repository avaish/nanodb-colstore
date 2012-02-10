CREATE TABLE test_pk_index (
    id INTEGER PRIMARY KEY,
    value VARCHAR(20)
);

INSERT INTO test_pk_index VALUES ( 10, 'red' );

INSERT INTO test_pk_index VALUES ( 15, 'orange' );

INSERT INTO test_pk_index VALUES ( 5, 'yellow' );

INSERT INTO test_pk_index VALUES ( 25, 'green' );
