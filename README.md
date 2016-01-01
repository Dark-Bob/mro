# MRO
[![Build Status][circleci-image]][circleci-url]
[circleci-url]: https://circleci.com/gh/Dark-Bob/mro
[circleci-image]: https://circleci.com/gh/Dark-Bob/mro.svg?style=shield

### Overview

Creating a way to simply interact with a predefined database schema in python. This package takes care of the vast majority of everyday calls and glue work you have to do to create a nice object oriented view of your database. 

### Getting The Package

```bash
pip install git+git://github.com/Dark-Bob/mro.git
```

### User Login Example 

They say a picture is worth a thousand words. Here's hoping a code example is worth a couple too. We're going to outline how to use **mro** for a login.

Below is a fairly simple user table defined in SQL. We assume that this has already been created in the database and populated with some data.

```sql
create table user (
id serial, 
last_login timestamp,
name varchar(64),
email varchar(256) unique,
password varchar(256),
primary key (id)
);
create index on user using hash (email);
```
Notice we're able to do things like create a hashed index for faster lookups of users via email easily.

Next we're on to the python code using **mro** for a login.

```python
import sys, psycopg2
from datetime import datetime
import mro

# create a connection to your postgres database and 
# load the database in mro before making any other mro calls
mro.load_database(psycopg2.connect(database='circle_test', user='ubuntu'))
# that's it all your tables in your database are now ready to be used as classes

# passed in program arguments
email = sys.argv[0]
password = sys.argv[1]

# validate a user using mro
# here we select one record where the field email matches the local variable
# if there is a match user will be populated if not it will be None
user = mro.user.select_one("email = " + email.lower())
if user is None:
    print("A user with that email does not exist in the database")
else:
    # in real life store a hashed password in the database
    # notice how we can just use column names as attributes
    if user.password != password:
        print("Password mismatch")
    else:
        # success we found a user with the provided email and the password matched
        print("Welcome back {}", user.name)
        # update last logged in time for this user in the database
        user.last_login = datetime.now()
```
###### Points to note:
- More example usage can be found in the tests, if anything is still not very obvious please raise an issue and I'll create a tutorial
- Looking at the last line of the example you may be worried that to update several columns you'd make a database call for each one but there is an update function that can be used as below with an arbitrary number of column names. There are similar calls for multiple row inserts, etc.
```
        user.update(last_login = datetime.now(), name = "Molly")
```
- I need to make the where clause sql injection safe. It's on the plan, just not got there yet. If this is an issue please raise and I'll prioritise.

### MRO vs Traditional ORM

These days especially data tends to outlive specific code versions and you may have mutliple versions of code talking to the same DB as you migrate your userbase. By having the database own its schema and the application just reflecting and using the bits it needs, it's much easier to extend. In a multiple application using one databse environemnt typical of SaaS which application should even be the owner of the schema? **mro** removes arbitrary decisions like that while allowing a much simpler data migration strategy in most cases, especially the complex ones. I will attempt to cover using views and stored procs to facilitate big migrations in future.

Some of the other ORM packages SqlAlchemy, SqlObject, etc. support reverse ORM to some extent but IMHO don't do a very good job of it as that's not the use case they were designed for. **mro** is extremely simple to get going because it's left a lot of the heavy lifting where it should be in the database.

### Supported Databases
- So far it's only Postgres

### Todos
- Make it SQL injection safe
- Support multiple schemas
- Support stored procs
- Support views




