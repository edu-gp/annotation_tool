import logging
import os
import sys
from envparse import env
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
from alchemy.db.model import metadata
from alembic import context

sys.path = ["", ".."] + sys.path[1:]

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config
db_url = env("DB_URL_FOR_MIGRATION", None)
if db_url:
    logging.info("DB_URL is {}".format(db_url))
    config.set_main_option("sqlalchemy.url", db_url)
else:
    raise ValueError("DB URL is not specified.")

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

target_metadata = metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    # For sqlite3 we need to enable batch mode (only work with online mode.
    # Offline has a different configuration.) This does not effect other
    # databases.
    # https://github.com/miguelgrinberg/Flask-Migrate/issues/252
    # https://alembic.sqlalchemy.org/en/latest/batch.html#batch-mode-with
    # -autogenerate
    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata, render_as_batch=True
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
