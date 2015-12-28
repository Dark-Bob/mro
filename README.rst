MRO
===

Overview
--------

Creating a way to simply interact with a predefined database schema in python. This package takes care of the vast majority of everyday calls and glue work you have to do to create a nice object oriented view of your database. 

MRO vs Traditional ORM
----------------------

These days especially data tends to outlive specific code versions and you may have mutliple versions of code talking to the same DB as you migrate your userbase. By having the database own its schema and the application just reflecting and using the bits it needs, it's much easier to extend. In a multiple application using one databse environemnt typical of SaaS which application should even be the owner of the schema? MRO removes arbitrary decisions like that while allowing a much simpler data migration strategy in most cases, especially the complex ones. I will attempt to cover using views and stored procs to facilitate big migrations in future.

Some of the other ORM packages SqlAlchemy, SqlObject, etc. support reverse ORM to some extent but IMHO don't do a very good job of it as that's not the use case they were designed for. MRO is extremely simple to get going because it's left a lot of the heavy lifting where it should be in the database.

Supported Databases
-------------------

So far it's only Postgres