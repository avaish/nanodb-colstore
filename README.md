NanoDB Column Store
-------------------

This is a project designed to enhance NanoDB to work as a column store
database. The project will implement this by creating a new Table Manager to
work with the new data files for column oriented tables, making changes to the
Storage Manager and Buffer Manager as necessary. Work will then commence on a
query planner so OLAP style queries can be executed on the database.