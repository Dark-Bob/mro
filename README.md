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

If the package has been updated and you want to pull a newer version:

```bash
pip install --upgrade --no-deps --force-reinstall git+git://github.com/Dark-Bob/mro.git
```

### Simple Select Example 

They say a picture is worth a thousand words. Here's hoping a code example is worth a couple too. We're going to outline how to use **mro** to select
some data from our database. 

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


```python
import sys, psycopg2
from datetime import datetime
import mro

# pass a function to create a connection to your postgres database
# and load the database in mro before making any other mro calls
mro.load_database(lambda: psycopg2.connect(database='circle_test', user='ubuntu'))
# that's it all your tables in your database are now ready to be used as classes

# Select a count of users that have logged in since yesterday, when dealing with user input
# in your queries, please use the pyformat syntax as explained in the user login example below
user_login_count = mro.user.select_count("last_login > TIMESTAMP 'yesterday'")
if user_login_count is None:
    print("No users have logged in today...")
else:
    print("There have been {} user logins today.".format(user_login_count))
```

#### Queries with User Input
###### User Login

Next we're on to the python code using **mro** for a login.

**N.B** When dealing with user input you should use the pyformat syntax to pass in variables to your queries

```python
import sys, psycopg2
from datetime import datetime
import mro

# pass a function to create a connection to your postgres database
# and load the database in mro before making any other mro calls
mro.load_database(lambda: psycopg2.connect(database='circle_test', user='ubuntu'))
# that's it all your tables in your database are now ready to be used as classes

# passed in program arguments
email = sys.argv[0]
password = sys.argv[1]

# validate a user using mro
# here we select one record where the field email matches the local variable
# if there is a match user will be populated if not it will be None
# This uses a pyformat string, the parameters to the method,
# should match the pyformat string.
user = mro.user.select_one("email = %s", email.lower())
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
###### Tests
- Create a venv and install the mro package requirements to this venv
- To run the tests you will need to have a local postgres running, to do this run the following command:

```bash
docker run -it -d -p 5432:5432 --name database postgres
```

The create a file in your project root directory called my_connection with a function like below:

```python
import psycopg2


def get_connection():
    return psycopg2.connect(host='localhost', user='postgres')
```

After this has been setup after reboots the database can be started with.

```bash
docker start database
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
- Support multiple schemas
- Support stored procs
- Support views



