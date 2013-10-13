#!/usr/bin/env python
#
# Copyright Adrian Toader
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""
	A lightweight wrapper around MySQLdb.
"""

from __future__ import absolute_import, division, with_statement

import itertools
import logging
import os
import time

try:
	from mysql import connector as MySQLConnector
except ImportError:
	# If mysql (http://dev.mysql.com/downloads/connector/python/)
	#isn't available this module won't actually be useable,
	# but we want it to at least be importable on readthedocs.org,
	# which has limitations on third-party modules.
	if 'READTHEDOCS' in os.environ:
		MySQLConnector = None
	else:
		raise

version = '1.0'
version_info = (1, 0, 0, 0)


class Connection(object):
	""" a lightweight wrapper around MySQLdb connections """

	def __init__(self, host, database, user=None, password=None,
				 max_idle_time=8 * 3600, connect_timeout=4,
				 time_zone="+0:00"):
		"""  """

		self.host = host
		self.database = database
		self.max_idle_time = float(max_idle_time)

		args = dict(use_unicode=True, charset="utf8",
					db=database, time_zone=time_zone,
					connect_timeout=connect_timeout, sql_mode='TRADITIONAL')
		if user is not None:
			args['user'] = user
		if password is not None:
			args['passwd'] = password

		# We accept a path to a MySQL socket file or a host(:port) string
		if "/" in host:
			args['unix_socket'] = host
		else:
			self.socket = None
			pair = host.split(':')
			if len(pair) == 2:
				args['host'] = pair[0]
				args['port'] = int(pair[1])
			else:
				args['host'] = host
				args['port'] = 3306

		self._db = None
		self._db_args = args
		self._last_use_time = time.time()
		try:
			self.reconnect()
		except Exception:
			logging.error('Cannot connect to MySQL on %s', self.host,
						  exc_info=True)

	def __del__(self):
		"""  """

		self.close()

	def close(self):
		""" closes this database connection """

		if getattr(self, '_db', None) is not None:
			self._db.close()
			self._db = None

	def reconnect(self):
		""" closes the existing database connection and re-opens it """

		self.close()
		self._db = MySQLConnector.Connect(**self._db_args)
		self._db.autocommit = True

	def iter(self, query, *parameters, **kwparameters):
		""" returns an iterator for the given query and parameters """

		self._ensure_connected()
		cursor = MySQLConnector.cursor.MySQLCursor(self._db)
		try:
			self._execute(cursor, query, parameters, kwparameters)
			column_names = [d[0] for d in cursor.description]
			for row in cursor:
				yield Row(zip(column_names, row))
		finally:
			cursor.close()

	def query(self, query, *parameters, **kwparameters):
		""" returns a row list for the given query and parameters """

		cursor = self._cursor()
		try:
			self._execute(cursor, query, parameters, kwparameters)
			column_names = [d[0] for d in cursor.description]
			return [Row(zip(column_names, row)) for row in cursor]
			#return [Row(izip(column_names, row)) for row in cursor] # python2
		finally:
			cursor.close()

	def get(self, query, *parameters, **kwparameters):
		""" returns the first row returned for the given query """

		rows = self.query(query, *parameters, **kwparameters)
		if not rows:
			return None
		elif len(rows) > 1:
			raise Exception('Multiple rows returned for Database.get() query')
		else:
			return rows[0]

	# rowcount is a more reasonable default return value than lastrowid,
	# but for historical compatibility execute() must return lastrowid.
	def execute(self, query, *parameters, **kwparameters):
		""" executes the given query, returning the lastrowid from the query """

		return self.execute_lastrowid(query, *parameters, **kwparameters)

	def execute_lastrowid(self, query, *parameters, **kwparameters):
		""" executes the given query, returning the lastrowid from the query """

		cursor = self._cursor()
		try:
			self._execute(cursor, query, parameters, kwparameters)
			return cursor.lastrowid
		finally:
			cursor.close()

	def execute_rowcount(self, query, *parameters, **kwparameters):
		""" executes the given query, returning the rowcount from the query """

		cursor = self._cursor()
		try:
			self._execute(cursor, query, parameters, kwparameters)
			return cursor.rowcount
		finally:
			cursor.close()

	def executemany(self, query, parameters):
		""" executes the given query against all the given param sequences and
		return the lastrowid from the query """

		return self.executemany_lastrowid(query, parameters)

	def executemany_lastrowid(self, query, parameters):
		""" executes the given query against all the given param sequences and
		return the lastrowid from the query """

		cursor = self._cursor()
		try:
			cursor.executemany(query, parameters)
			return cursor.lastrowid
		finally:
			cursor.close()

	def executemany_rowcount(self, query, parameters):
		""" executes the given query against all the given param sequences and
		return the rowcount from the query """

		cursor = self._cursor()
		try:
			cursor.executemany(query, parameters)
			return cursor.rowcount
		finally:
			cursor.close()


	update = execute_rowcount
	updatemany = executemany_rowcount

	insert = execute_lastrowid
	insertmany = executemany_lastrowid


	def _ensure_connected(self):
		""" mysql by default closes client connections that are idle for
		8 hours, but the client library does not report this fact until
		you try to perform a query and it fails.  Protect against this
		case by preemptively closing and reopening the connection
		if it has been idle for too long (7 hours by default) """

		if (self._db is None or
			(time.time() - self._last_use_time > self.max_idle_time)):
			self.reconnect()
		self._last_use_time = time.time()

	def _cursor(self):
		"""  """

		self._ensure_connected()
		return self._db.cursor()

	def _execute(self, cursor, query, parameters, kwparameters):
		"""  """

		try:
			return cursor.execute(query, kwparameters or parameters)
		except OperationalError:
			logging.error('error connecting to MySQL on %s', self.host)
			self.close()
			raise


class Row(dict):
	""" a dict that allows for object-like property access syntax """

	def __getattr__(self, name):
		try:
			return self[name]
		except KeyError:
			raise AttributeError(name)


# Alias some common MySQL exceptions
IntegrityError = MySQLConnector.IntegrityError
OperationalError = MySQLConnector.OperationalError

