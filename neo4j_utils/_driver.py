#!/usr/bin/env python

#
# Copyright 2021-2022, Heidelberg University Hospital
#
# File author(s): Denes Turei <turei.denes@gmail.com>
#                 Sebastian Lobentanzer
#
# Distributed under the MIT (Expat) license, see the file `LICENSE`.
#

"""
Neo4j connection management and CYPHER interface.

A wrapper around the Neo4j driver which handles the DBMS connection and
provides basic management methods.
"""

from __future__ import annotations

from ._logger import logger, log_traceback

logger.debug(f'Loading module {__name__.strip("_")}.')

from typing import Literal
import os
import re
import builtins
import warnings
import importlib as imp
import itertools
import contextlib

import yaml
import neo4j
import appdirs
import neo4j.exceptions as neo4j_exc

import neo4j_utils._misc as _misc
import neo4j_utils._print as printer
import neo4j_utils._query as _query

__all__ = ['CONFIG_FILES', 'DEFAULT_CONFIG', 'Driver']


CONFIG_FILES = Literal['neo4j.yaml', 'neo4j.yml']
DEFAULT_CONFIG = {
    'user': 'neo4j',
    'passwd': 'neo4j',
    'db': 'neo4j',
    'uri': 'neo4j://localhost:7687',
    'fetch_size': 1000,
    'raise_errors': False,
    'fallback_db': ('system', 'neo4j'),
    'fallback_on': ('TransientError',),
}


class Driver:
    """
    Manage the connection to the Neo4j server.
    """

    _connect_essential = ('uri', 'user', 'passwd')
    version: str

    def __init__(
            self,
            driver: neo4j.Driver | Driver | None = None,
            db_name: str | None = None,
            db_uri: str | None = None,
            db_user: str | None = None,
            db_passwd: str | None = None,
            config: CONFIG_FILES | None = None,
            fetch_size: int = 1000,
            raise_errors: bool | None = None,
            wipe: bool = False,
            offline: bool = False,
            fallback_db: str | tuple[str] | None = None,
            fallback_on: str | set[str] | None = None,
            multi_db: bool | None = None,  # legacy parameter for pre-4.0 DBs
            **kwargs
    ):
        """
        Create a Driver object with database connection and runtime parameters.

        Establishes the connection and executes queries. A wrapper around
        the `Driver` object from the :py:mod:`neo4j` module, which is stored
        in the :py:attr:`driver` attribute.

        The connection can be defined in three ways:
            * Providing a ready ``neo4j.Driver`` instance
            * By URI and authentication data
            * By a YAML config file

        Args:
            driver:
                A ``neo4j.Driver`` instance, created by, for example,
                ``neo4j.GraphDatabase.driver``.
            db_name:
                Name of the database (Neo4j graph) to use.
            db_uri:
                Protocol, host and port to access the Neo4j server.
            db_user:
                Neo4j user name.
            db_passwd:
                Password of the Neo4j user.
            fetch_size:
                Optional; the fetch size to use in database transactions.
            raise_errors:
                Raise the errors instead of turning them into log messages
                and returning `None`.
            config:
                Path to a YAML config file which provides the URI, user
                name and password.
            wipe:
                Wipe the database after connection, ensuring the data is
                loaded into an empty database.
            offline:
                Disable any interaction to the server. Queries won't be
                executed. The config will be still stored in the object
                and it will be ready to go online by its ``go_online``
                method.
            fallback_db:
                Arbitrary number of fallback databases. If a query fails
                to run against the current database, it will be attempted
                against the fallback databases.
            fallback_on:
                Switch to the fallback databases upon these errors. At most
                of the errors it doesn't make sense to try to run with
                another database, but especially for database and server
                management commands this is a convenient solution.
            multi_db:
                Not sure what is this for, biocypher requires it.
            kwargs:
                Ignored.
        """

        self.driver = getattr(driver, 'driver', driver)
        self._db_config = {
            'uri': db_uri,
            'user': db_user,
            'passwd': db_passwd,
            'db': db_name,
            'fetch_size': fetch_size,
            'raise_errors': raise_errors,
            'fallback_db': fallback_db,
            'fallback_on': fallback_on,
        }
        self._config_file = config
        self._drivers = {}
        self._queries = {}
        self._offline = offline
        self.multi_db = multi_db

        if self.driver:

            logger.info('Using the driver provided.')
            self._config_from_driver()
            self._register_current_driver()

        else:

            logger.info(
                'No driver provided, initialising '
                'it from local config.',
            )
            self.db_connect()

        self.get_neo4j_version()

        self.ensure_db()

        if wipe:

            self.wipe_db()


    def reload(self):
        """
        Reloads the object from the module level.
        """

        modname = self.__class__.__module__
        mod = __import__(modname, fromlist=[modname.split('.')[0]])
        imp.reload(mod)
        new = getattr(mod, self.__class__.__name__)
        self.__class__ = new


    def db_connect(self):
        """
        Connect to the database server.

        Creates a database connection manager (driver) based on the
        current configuration.
        """

        if not self._connect_param_available:

            self.read_config()

        con_param = printer.dict_str({'uri': self.uri, 'auth': self.auth})
        logger.info(f'Attempting to connect: {con_param}')

        if self.offline:

            self.driver = None
            logger.info('Offline mode, not connecting to database.')

        else:

            self.driver = neo4j.GraphDatabase.driver(
                uri = self.uri,
                auth = self.auth,
            )
            logger.info('Opened database connection.')


        self._register_current_driver()


    @property
    def _connect_param_available(self):
        """
        Check for essential connection parameters.

        Are all parameters available that are essential for establishing
        a connection?
        """

        return all(
            self._db_config.get(k, None)
            for k in self._connect_essential
        )


    @property
    def status(self) -> Literal[
            'no driver',
            'no connection',
            'db offline',
            'db online',
            'offline',
    ]:
        """
        State of this driver object and its current database.
        """

        if self.offline:

            return 'offline'

        if not self.driver:

            return 'no driver'

        db_status = self.db_status()

        return f'db {db_status}' if db_status else 'no connection'


    @property
    def uri(self):
        """
        Database server URI (from config or built-in default).
        """

        return self._db_config.get('uri', 0) or DEFAULT_CONFIG['uri']


    @property
    def auth(self):
        """
        Database server user and password (from config or built-in default).
        """

        return (
            tuple(self._db_config.get('auth', ())) or
            (
                self._db_config.get('user', 0) or DEFAULT_CONFIG['user'],
                self._db_config.get('passwd', 0) or DEFAULT_CONFIG['passwd'],
            )
        )


    def read_config(self, section: str | None = None):
        """
        Read the configuration from a YAML file.

        Populates the instance configuration from one section of a YAML
        config file.
        """

        config_key_synonyms = {
            'password': 'passwd',
            'pw': 'passwd',
            'username': 'user',
            'login': 'user',
            'host': 'uri',
            'address': 'uri',
            'server': 'uri',
            'graph': 'db',
            'database': 'db',
            'name': 'db',
        }

        if not self._config_file or not os.path.exists(self._config_file):

            confdirs = ('.', appdirs.user_config_dir('neo4j-utils', 'saezlab'))
            conffiles = CONFIG_FILES.__args__

            for config_path_t in itertools.product(confdirs, conffiles):

                config_path_s = os.path.join(*config_path_t)

                if os.path.exists(config_path_s):

                    self._config_file = config_path_s

        if self._config_file and os.path.exists(self._config_file):

            logger.info('Reading config from `%s`.' % self._config_file)

            with open(self._config_file) as fp:

                conf = yaml.safe_load(fp.read())

            for k, v in conf.get(section, conf).items():

                k = k.lower()
                k = config_key_synonyms.get(k, k)

                if not self._db_config.get(k, None):

                    self._db_config[k] = v

        elif not self._connect_param_available:

            logger.warn('No config available, falling back to defaults.')

        self._config_from_defaults()


    def _config_from_driver(self):

        from_driver = {
            'uri': self._uri(
                host = getattr(self.driver, 'default_host', None),
                port = getattr(self.driver, 'default_port', None),
            ),
            'db': self.current_db,
            'fetch_size': getattr(
                getattr(self.driver, '_default_workspace_config', None),
                'fetch_size',
                None,
            ),
            'user': self.user,
            'passwd': self.passwd,
        }

        for k, v in from_driver.items():

            self._db_config[k] = self._db_config.get(k, v) or v

        self._config_from_defaults()


    def _config_from_defaults(self):
        """
        Populates missing config items by their default values.
        """

        for k, v in DEFAULT_CONFIG.items():

            if self._db_config.get(k, None) is None:

                self._db_config[k] = v


    def _register_current_driver(self):

        self._drivers[self.current_db] = self.driver


    @staticmethod
    def _uri(
            host: str = 'localhost',
            port: str | int = 7687,
            protocol: str = 'neo4j',
    ) -> str:

        return f'{protocol}://{host}:{port}/'


    def close(self):
        """
        Closes the Neo4j driver if it exists and is open.
        """

        if hasattr(self, 'driver') and hasattr(self.driver, 'close'):

            self.driver.close()


    def __del__(self):

        self.close()


    @property
    def home_db(self) -> str | None:
        """
        Home database of the current user.
        """

        return self._db_name()


    @property
    def default_db(self) -> str | None:
        """
        Default database of the server.
        """

        return self._db_name('DEFAULT')


    def _db_name(
            self,
            which: Literal['HOME', 'DEFAULT'] = 'HOME',
    ) -> str | None:

        try:

            resp, summary = self.query(
                f'SHOW {which} DATABASE;',
                fallback_db = self._get_fallback_db,
            )

        except (neo4j_exc.AuthError, neo4j_exc.ServiceUnavailable) as e:

            logger.error(
                f'No connection to Neo4j server: {printer.error_str(e)}',
            )
            return

        if resp:

            return resp[0]['name']


    @property
    def _get_fallback_db(self) -> tuple[str]:

        return _misc.to_tuple(
            getattr(self, '_fallback_db', None) or
            self._db_config['fallback_db'],
        )


    @property
    def _get_fallback_on(self) -> set[str]:

        return _misc.to_set(
            getattr(self, '_fallback_on', None) or
            self._db_config['fallback_on'],
        )


    def query(
            self,
            query: str,
            db: str | None = None,
            fetch_size: int | None = None,
            write: bool = True,  # route to write server (default)
            explain: bool = False,
            profile: bool = False,
            fallback_db: str | tuple[str] | None = None,
            fallback_on: str | set[str] | None = None,
            raise_errors: bool | None = None,
            **kwargs,
    ) -> tuple[list[dict] | None, neo4j.work.summary.ResultSummary | None]:
        """
        Run a CYPHER query.

        Create a session with the wrapped driver, run a CYPHER query and
        return the response.

        Args:
            query:
                A valid CYPHER query, can include APOC if the APOC
                plugin is installed in the accessed database.
            db:
                The DB inside the Neo4j server that should be queried
                fetch_size (int): the Neo4j fetch size parameter.
            write:
                Indicates whether to address write- or read-servers.
            explain:
                Indicates whether to EXPLAIN the CYPHER query and
                return the ResultSummary.
            profile:
                Indicates whether to PROFILE the CYPHER query and
                return the ResultSummary.
            fallback_db:
                If the query fails due to the database being unavailable,
                try to execute it against a fallback database. Typically
                the default database "neo4j" can be used as a fallback.
            fallback_on:
                Run queries against the fallback databases in case of
                these errors.
            raise_errors:
                Raise Neo4j errors instead of only printing them into
                the log and stdout.
            **kwargs:
                Optional objects used in CYPHER interactive mode,
                for instance for passing a parameter dictionary.

        Returns:
            2-tuple:
                - neo4j.Record.data: the Neo4j response to the query, consumed
                  by the shorthand ``.data()`` method on the ``Result`` object
                - neo4j.ResultSummary: information about the result returned
                  by the ``.consume()`` method on the ``Result`` object

        Todo:

            - generalise? had to create conditionals for profiling, as
              the returns are not equally important. the .data()
              shorthand may not be applicable in all cases. should we
              return the `Result` object directly plus the summary
              object from .consume()?

                - From Docs: "Any query results obtained within a
                  transaction function should be consumed within that
                  function, as connection-bound resources cannot be
                  managed correctly when out of scope. To that end,
                  transaction functions can return values but these
                  should be derived values rather than raw results."

            - use session.run() or individual transactions?

                - From Docs: "Transaction functions are the recommended
                  form for containing transactional units of work.
                  When a transaction fails, the driver retry logic is
                  invoked. For several failure cases, the transaction
                  can be immediately retried against a different
                  server. These cases include connection issues,
                  server role changes (e.g. leadership elections)
                  and transient errors."

            - use write and read distinctions in calling transactions
              ("access mode")?
            - use neo4j `@unit_of_work`?

        """

        if explain:

            query = 'EXPLAIN ' + query

        elif profile:

            query = 'PROFILE ' + query

        if self.offline:

            logger.info(f'Offline mode, not running query: `{query}`.')

            return None, None
        if 'CREATE' not in query and 'SHOW DATABASES' not in query:
            db = db or self._db_config['db'] or neo4j.DEFAULT_DATABASE
        fetch_size = fetch_size or self._db_config['fetch_size']
        raise_errors = (
            self._db_config['raise_errors']
                if raise_errors is None else
            raise_errors
        )

        if self.multi_db:
            session_args = {
                'database': db,
                'fetch_size': fetch_size,
                'default_access_mode':
                    neo4j.WRITE_ACCESS if write else neo4j.READ_ACCESS,
            }
        else:
            session_args = {
                'fetch_size': fetch_size,
                'default_access_mode':
                    neo4j.WRITE_ACCESS if write else neo4j.READ_ACCESS,
            }

        try:

            with self.session(**session_args) as session:

                res = session.run(query, **kwargs)

                self._queries['last'] = _query.Query(
                    query = query,
                    args = kwargs,
                )

                return res.data(), res.consume()

        except (neo4j_exc.Neo4jError, neo4j_exc.DriverError) as e:

            fallback_db = fallback_db or getattr(self, '_fallback_db', ())
            fallback_on = _misc.to_set(
                _misc.if_none(
                    fallback_on,
                    self._get_fallback_on,
                ),
            )

            if self.match_error(e, fallback_on):

                for fdb in _misc.to_tuple(fallback_db):

                    if fdb != db:

                        logger.warn(
                            'Running query against fallback '
                            f'database `{fdb}`.',
                        )

                        return self.query(
                            query = query,
                            db = fdb,
                            fetch_size = fetch_size,
                            write = write,
                            fallback_on = set(),
                            raise_errors = raise_errors,
                            **kwargs
                        )

            logger.error(f'Failed to run query: {printer.error_str(e)}')
            logger.error(f'The error happened with this query: {query}')
            log_traceback()

            self._queries['last_failed'] = _query.Query(
                query = query,
                args = kwargs,
            )

            if e.__class__.__name__ == 'AuthError':

                logger.error(
                    'Authentication error, switching to offline mode.',
                )
                self.go_offline()

            if raise_errors:

                raise

            return None, None


    def explain(
            self,
            query,
            db=None,
            fetch_size=None,
            write=True,
            **kwargs,
    ):
        """
        Explain a query and pretty print the output.

        CAVE: Only handles linear profiles (no branching) as of now.
        TODO include branching as in profile()
        """

        logger.info('Explaining a query.')

        data, summary = self.query(
            query,
            db,
            fetch_size,
            write,
            explain=True,
            **kwargs
        )

        plan = summary.plan
        printout = printer.pretty(plan)

        return plan, printout


    def profile(
            self,
            query,
            db=None,
            fetch_size=None,
            write=True,
            **kwargs,
    ):
        """
        Profile a query and pretty print the output.

        Args:
            query (str): a valid Cypher query (see :meth:`query()`)
            db (str): the DB inside the Neo4j server that should be queried
            fetch_size (int): the Neo4j fetch size parameter
            write (bool): indicates whether to address write- or read-
                servers
            explain (bool): indicates whether to ``EXPLAIN`` the CYPHER
                query and return the ResultSummary
            explain (bool): indicates whether to ``PROFILE`` the CYPHER
                query and return the ResultSummary
            **kwargs: optional objects used in CYPHER interactive mode,
                for instance for passing a parameter dictionary

        Returns:
            2-tuple:
                - dict: the raw profile returned by the Neo4j bolt driver
                - list of str: a list of strings ready for printing
        """

        logger.info('Profiling a query.')

        data, summary = self.query(
            query, db, fetch_size, write, profile=True, **kwargs
        )

        prof = summary.profile
        exec_time = (
            summary.result_available_after + summary.result_consumed_after
        )

        # get structure
        # TODO (readability may be better when ordered from top to bottom)

        # get print representation
        header = f'Execution time: {exec_time:n}\n'
        printout = printer.pretty(prof, [header], indent=0)

        return prof, printout


    @property
    def current_db(self) -> str:
        """
        Name of the current database.

        All operations and queries are executed by default on this database.

        Returns:
            Name of a database.
        """

        return self._db_config['db'] or self._driver_con_db or self.home_db


    @current_db.setter
    def current_db(self, name: str):
        """
        The database currently in use.

        Args:
            name:
                Name of a database.
        """

        self._db_config['db'] = name
        self.db_connect()


    @property
    def _driver_con_db(self):

        if not self.driver:

            return

        with warnings.catch_warnings():

            warnings.simplefilter('ignore')

            try:

                driver_con = self.driver.verify_connectivity()

            except neo4j_exc.ServiceUnavailable:

                logger.error('Can not access Neo4j server.')
                return

        if driver_con:

            first_con = next(driver_con.values().__iter__())[0]

            return first_con.get('db', None)


    def db_exists(self, name=None):
        """
        Tells if a database exists in the storage of the Neo4j server.

        Args:
            name (str): Name of a database (graph).

        Returns:
            (bool): `True` if the database exists.
        """

        return bool(self.db_status(name=name))


    def db_status(
            self,
            name: str | None = None,
            field: str = 'currentStatus',
    ) -> Literal['online', 'offline'] | str | dict | None:
        """
        Tells the current status or other state info of a database.

        Args:
            name:
                Name of a database (graph).
            field:
                The field to return.

        Returns:
            The status as a string, `None` if the database
            does not exist. If :py:attr:`field` is `None` a
            dictionary with all fields will be returned.
        """

        name = name or self.current_db

        query = 'SHOW DATABASES'

        with self.fallback():
            resp, summary = self.query(query=query)

        for record in resp:
            if name == record['name']:
                return record.get(field, None)


    def db_online(self, name: str | None = None):
        """
        Tells if a database is currently online (active).

        Args:
            name (str): Name of a database (graph).

        Returns:
            (bool): `True` if the database is online.
        """

        return self.db_status(name=name) == 'online'


    def create_db(self, name: str | None = None):
        """
        Create a database if it does not already exist.

        Args:
            name (str): Name of the database.
        """

        self._manage_db('CREATE', name=name, options='IF NOT EXISTS')


    def start_db(self, name: str | None = None):
        """
        Starts a database (brings it online) if it is offline.

        Args:
            name: Name of the database.
        """

        self._manage_db('START', name=name)


    def stop_db(self, name: str | None = None):
        """
        Stops a database, making sure it's offline.

        Args:
            name: Name of the database.
        """

        self._manage_db('STOP', name=name)


    def drop_db(self, name: str | None = None):
        """
        Deletes a database if it exists.

        Args:
            name: Name of the database.
        """

        self._manage_db('DROP', name=name, options='IF EXISTS')


    def _manage_db(
            self,
            cmd: Literal['CREATE', 'START', 'STOP', 'DROP'],
            name: str | None = None,
            options: str | None = None,
    ):
        """
        Executes a database management command.

        Args:
            cmd:
                The command: CREATE, START, STOP, DROP, etc.
            name:
                Name of the database.
            options:
                The optional parts of the command, following the database name.
        """

        self.query(
            '{} DATABASE {} {};'.format(
                cmd,
                name or self.current_db,
                options or '',
            ),
            fallback_db = self._get_fallback_db,
        )


    def wipe_db(self):
        """
        Delete all contents of the current database.

        Used in initialisation, deletes all nodes and edges and drops
        all indices and constraints.
        """

        logger.info(f'Wiping database `{self.current_db}`.')

        self.query('MATCH (n) DETACH DELETE n;')

        self.drop_indices_constraints()


    def ensure_db(self):
        """
        Makes sure the database exists and is online.

        If the database creation or startup is necessary but the user does
        not have the sufficient privileges, an exception will be raised.
        """

        if not self.db_exists():

            self.create_db()

        if not self.db_online():

            self.start_db()


    def select_db(self, name: str):
        """
        Set the current database.

        The Python driver is able to run only CYPHER statements, not Neo4j
        commands, hence we can't simply do ``:use database;``, but we
        create or re-use another `Driver` object.
        """

        current = self.current_db

        if current != name:

            self._register_current_driver()
            self._db_config['db'] = name

            if name in self._drivers:

                self.driver = self._drivers[name]

            else:

                self.db_connect()


    @property
    def indices(self) -> list | None:
        """
        List of indices in the current database.
        """

        return self._list_indices('indices')


    @property
    def constraints(self) -> list | None:
        """
        List of constraints in the current database.
        """

        return self._list_indices('constraints')


    def drop_indices_constraints(self):
        """
        Drops all indices and constraints in the current database.

        Requires the database to be empty.
        """

        major_neo4j_version = int(self.neo4j_version.split('.')[0])

        if major_neo4j_version >= 5:
            self.drop_constraints()

        else:
            self.drop_constraints()
            self.drop_indices()


    def drop_constraints(self):
        """
        Drops all constraints in the current database.

        Requires the database to be empty.
        """

        self._drop_indices(what = 'constraints')


    def drop_indices(self):
        """
        Drops all indices in the current database.

        Requires the database to be empty.
        """

        self._drop_indices(what = 'indexes')

    def _drop_indices(
            self,
            what: Literal['indexes', 'indices', 'constraints'] = 'constraints',
    ):

        what_u = self._idx_cstr_synonyms(what)

        major_neo4j_version = int(self.neo4j_version.split('.')[0])

        with self.session() as s:

            try:

                if major_neo4j_version >= 5:
                    indices = s.run(f'SHOW {what}')
                else:
                    indices = s.run(f'CALL db. {what}')

                indices = list(indices)
                n_indices = len(indices)
                index_names = ', '.join(i['name'] for i in indices)

                for idx in indices:
                    s.run(f'DROP {what_u} `{idx["name"]}` IF EXISTS')

                logger.info(f'Dropped {n_indices} indices: {index_names}.')

            except (neo4j_exc.Neo4jError, neo4j_exc.DriverError) as e:

                logger.error(f'Failed to run query: {printer.error_str(e)}')


    def _list_indices(
            self,
            what: Literal['indexes', 'indices', 'constraints'] = 'constraints',
    ) -> list | None:

        what_u = self._idx_cstr_synonyms(what)

        with self.session() as s:

            try:

                return list(s.run(f'SHOW {what_u.upper()};'))

            except (neo4j_exc.Neo4jError, neo4j_exc.DriverError) as e:

                logger.error(f'Failed to run query: {printer.error_str(e)}')


    @staticmethod
    def _idx_cstr_synonyms(what: str) -> str | None:

        what_s = {
            'indexes': 'INDEX',
            'indices': 'INDEX',
            'constraints': 'CONSTRAINT',
        }

        what_u = what_s.get(what, None)

        if not what_u:

            msg = (
                'Allowed keywords are: "indexes", '
                f'"indices" or "constraints", not `{what}`.'
            )

            logger.log_error(msg)

            raise ValueError(msg)

        return what_u


    @property
    def node_count(self) -> int | None:
        """
        Number of nodes in the database.
        """

        res, summary = self.query('MATCH (n) RETURN COUNT(n) AS count;')

        return res[0]['count'] if res else None


    @property
    def edge_count(self) -> int | None:
        """
        Number of edges in the database.
        """

        res, summary = self.query('MATCH ()-[r]->() RETURN COUNT(r) AS count;')

        return res[0]['count'] if res else None


    @property
    def user(self) -> str | None:
        """
        User for the currently active connection.

        Returns:
            The name of the user, `None` if no connection or no
            unencrypted authentication data is available.
        """

        return self._extract_auth[0]

    @property
    def passwd(self) -> str | None:
        """
        Password for the currently active connection.

        Returns:
            The name of the user, `None` if no connection or no
            unencrypted authentication data is available.
        """

        return self._extract_auth[1]


    @property
    def _extract_auth(self) -> tuple[str | None, str | None]:
        """
        Extract authentication data from the Neo4j driver.
        """

        auth = None, None

        if self.driver:

            opener_vars = self._opener_vars

            if 'auth' in opener_vars:

                auth = opener_vars['auth'].cell_contents

        return auth


    @property
    def _opener_vars(self) -> dict:
        """
        Extract variables from the opener part of the Neo4j driver.
        """

        return dict(
            zip(
                self.driver._pool.opener.__code__.co_freevars,
                self.driver._pool.opener.__closure__,
            ),
        )


    def __len__(self):

        return self.node_count


    @contextlib.contextmanager
    def use_db(self, name: str):
        """
        A context where the default database is set to `name`.

        Args:
            name:
                The name of the desired default database.
        """

        used_previously = self.current_db
        self.select_db(name = name)

        try:

            yield None

        finally:

            self.select_db(name = used_previously)


    @contextlib.contextmanager
    def fallback(
            self,
            db: str | tuple[str] | None = None,
            on: str | set[str] | None = None,
    ):
        """
        Should running on the default database fail, try a fallback database.

        A cotext that attempts to run queries against a fallback database if
        running against the default database fails.

        Args:
            db:
                Name of one or more fallback database(s).
            on:
                Names of the errors when the fallback database should be used.
        """

        prev = {}

        for var in ('db', 'on'):

            prev[var] = getattr(self, f'_fallback_{var}', None)
            setattr(
                self,
                f'_fallback_{var}',
                locals()[var] or self._db_config.get(f'fallback_{var}'),
            )

        try:

            yield None

        finally:

            for var in ('db', 'on'):

                setattr(self, f'_fallback_{var}', prev[var])


    @contextlib.contextmanager
    def session(self, **kwargs):
        """
        Context with a database connection session.

        A context that creates a session and closes it at the end.

        Args:
            Kwargs:
                Passed to ``neo4j.Neo4jDriver.session``.
        """

        session = self.driver.session(**kwargs)

        try:

            yield session

        finally:

            session.close()


    def __enter__(self):

        self._context_session = self.session()

        return self._context_session


    def __exit__(self, *exc):

        if hasattr(self, '_context_session'):

            self._context_session.close()
            delattr(self, '_context_session')


    def __repr__(self):

        return '<{} {}>'.format(
            self.__class__.__name__,
            self._connection_str if self.driver else '[offline]',
        )


    @property
    def _connection_str(self):

        return '%s://%s:%u/%s' % (
            re.split(
                r'(?<=[a-z])(?=[A-Z])',
                self.driver.__class__.__name__,
            )[0].lower(),
            self.driver._pool.address[0] if self.driver else 'unknown',
            self.driver._pool.address[1] if self.driver else 0,
            self.user or 'unknown',
        )


    @property
    def node_labels(self) -> list[str]:
        """
        Node labels defined in the database.

        Presence of a label does not guarantee any instance of it exists in
        the database.
        """

        return [
            i['label']
            for i in (self.query('CALL db.labels')[0] or [])
        ]


    @property
    def label_counts(self):
        """
        Count the nodes by labels.
        """

        return {
            r['LABELS(n)'][0]:
                r['COUNT(*)']
            for r in
            self.query(
                'MATCH (n) RETURN DISTINCT LABELS(n), COUNT(*);',
            )[0] or
            []
        }


    @property
    def rel_types(self) -> list[str]:
        """
        Relationship types defined in the database.
        """

        return [
            i['relationshipType']
            for i in (self.query('CALL db.relationshipTypes')[0] or [])
        ]


    @property
    def rel_type_counts(self):
        """
        Count the relationships by types.
        """

        return {
            r['TYPE(r)']:
                r['COUNT(*)']
            for r in
            self.query(
                'MATCH ()-[r]->() RETURN DISTINCT TYPE(r), COUNT(*);',
            )[0] or
            []
        }


    @property
    def prop_keys(self):
        """
        Property keys defined in the database.
        """

        return [
            i['propertyKey']
            for i in (self.query('CALL db.propertyKeys')[0] or [])
        ]


    @property
    def apoc_version(self) -> str | None:
        """
        Version of the APOC plugin available in the current database.
        """

        db = self._db_config['db'] or neo4j.DEFAULT_DATABASE

        try:

            with self.session(database = db) as session:

                res = session.run('RETURN apoc.version() AS output;')

                return res.data()[0]['output']

        except neo4j_exc.ClientError:

            return None


    @property
    def has_apoc(self) -> bool:
        """
        Tells if APOC is available in the current database.
        """

        return bool(self.apoc_version)


    def write_config(self, path: str = CONFIG_FILES.__args__[0]):
        """
        Write the current config into file.
        """

        with open(path, 'w') as fp:

            yaml.safe_dump(self._db_config, fp)


    @property
    def offline(self) -> bool:
        """
        Whether the driver is in offline mode.
        """

        return self._offline


    @offline.setter
    def offline(self, offline: bool):
        """
        Enable or disable offline mode.
        """

        self.go_offline() if offline else self.go_online()


    def go_offline(self):
        """
        Switch to offline mode.

        The connection will be destroyed and query execution or any contact
        to the server won't be attempted any more.
        """

        self._offline = True
        self.close()
        self.driver = None
        self._register_current_driver()
        logger.warn('Offline mode: any interaction to the server is disabled.')


    def go_online(
            self,
            db_name: str | None = None,
            db_uri: str | None = None,
            db_user: str | None = None,
            db_passwd: str | None = None,
            config: CONFIG_FILES | None = None,
            fetch_size: int | None = None,
            raise_errors: bool | None = None,
            wipe: bool = False,
    ):
        """
        Switch to online mode.

        In offline mode this object doesn't do any interaction to the server,
        instead it serves more or less as a mock. Here we attempt to create
        a connection to the server based on the config contained in this
        object or passed as arguments to this method. If the connection is
        successful, the :attr:``offline`` flag will be set to ``False`` and
        server interactions will be performed as normal.

        Args:
            db_name:
                Name of the database (Neo4j graph) to use.
            db_uri:
                Protocol, host and port to access the Neo4j server.
            db_user:
                Neo4j user name.
            db_passwd:
                Password of the Neo4j user.
            fetch_size:
                Optional; the fetch size to use in database transactions.
            raise_errors:
                Raise the errors instead of turning them into log messages
                and returning `None`.
            config:
                Path to a YAML config file which provides the URI, user
                name and password.
            wipe:
                Wipe the database after connection, ensuring the data is
                loaded into an empty database.
        """

        self._offline = False

        try:

            for k, current in self._db_config.items():

                self._db_config = {
                    k: _misc.if_none(
                        locals().get(k, None),
                        current,
                        DEFAULT_CONFIG[k],
                    )
                    for k, current in self._db_config.items()
                }

            self._config_file = self._config_file or config

            self.db_connect()
            self.ensure_db()
            logger.info('Online mode: ready to run queries.')

        except Exception as e:

            logger.error(f'Failed to connect: {printer.error_str(e)}')
            self._offline = True

        if wipe:

            self.wipe_db()


    @staticmethod
    def match_error(
            error: Exception | str,
            errors: set[Exception | str],
    ) -> bool:
        """
        Tells if error is listed in errors.

        Args:
            error:
                An exception type or its name as string.
            errors:
                One or more exception types or names.

        Returns:
            True if error is found to be a subclass of any of the classes
            listed in errors.
        """

        def str_to_exc(e):

            return (
                e.__class__
                    if isinstance(e, Exception) else
                getattr(builtins, e, getattr(neo4j_exc, e, e))
                if isinstance(e, str) else
                e
            )


        error = str_to_exc(error)
        errors = {str_to_exc(e) for e in _misc.to_set(errors)}

        return (
            error in errors or
            (
                isinstance(error, type) and
                any(
                    issubclass(error, e)
                    for e in errors
                    if isinstance(e, type)
                )
            )
        )


    @property
    def last_query(self):
        """
        The last succesful query.
        """

        return self._queries.get('last')


    @property
    def last_fail(self):
        """
        The last failed query.
        """

        return self._queries.get('last_failed')

    @property
    def neo4j_version(self) -> str:
        """
        Returns the neo4j version.
        """
        return self.version

    def get_neo4j_version(self):
        """Get neo4j version."""
        try:
            neo4j_version = self.query(
                """
                    CALL dbms.components()
                    YIELD name, versions, edition
                    UNWIND versions AS version
                    RETURN version AS version
                """,
            )[0][0]['version']
            self.version = neo4j_version
        except Exception as e:
            logger.warning(f'Error detecting Neo4j version: {e}')
