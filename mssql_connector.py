#!/usr/bin/env python

###
#  A Datastore Bulkloader connector for reading data from MS SQL Server.
###

from google.appengine.ext.bulkload import connector_interface
from google.appengine.ext.bulkload import bulkloader_errors
import pymssql
import os.path

class MSSQLConnector(connector_interface.ConnectorInterface):

  @classmethod
  def create_from_options(cls, options, name):
    columns = options.get('columns', None)
    if not columns:
        raise bulkloader_errors.InvalidConfiguration(
            'Sql query must be specified in the columns '
            'configuration option. (In transformer name %s.)' % name)

    return cls(columns)

  def __init__(self, sql_query):
    """Initializer.

    Args:
      sql_query: (required) select query which will be sent to database. The returned columns/aliases will be used as the connectors column names
    """
    self.sql_query = unicode(sql_query)

  def generate_import_record(self, filename, bulkload_state):

    dbprops = __import__(os.path.splitext(filename)[0])

    connection = pymssql.connect(dbprops.host, dbprops.uid, dbprops.pwd, dbprops.database)

    cursor = connection.cursor()
    cursor.execute(self.sql_query)
    num_fields = len(cursor.description)
    field_names = [i[0] for i in cursor.description]
    for row in cursor.fetchall():
       decoded_dict = {}
       for i in range(num_fields):
         decoded_dict[field_names[i]] = row[i]
       yield decoded_dict
    cursor.close()
    connection.close()
