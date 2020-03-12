#!/usr/bin/env python3

############################################################################
#
# check_mssql_proc - Checks various aspect of MSSQL servers
#
#
# Notes:
# In Progress script, will not work atm
#####################################################################

#TODO! remove if %prog works
#progname="check_mssql_proc";
version="v1.1"

import pymssql
import time
import sys
import re
from optparse import OptionParser, OptionGroup

LOGSHIP_QUERY = "exec dbo.{} @primary_host='{}',@primary_db='{}',@secondary_db='{}'"
LOGSPACE_MONITOR_QUERY = "exec dbo.{} @warning='{}',@critical='{}'"

MODES = {
	'usp_logshipdb_monitor'	:   {   	'help'      : 'Number of users connected',
                    		    		'query'     : LOGSHIP_QUERY
                     		    },
	'logspace_monitor' :        { 		'help'	    : 'Number of users connected',
						'query'     : LOGSPACE_MONITOR_QUERY
				    }
        }

def return_nagios(options, stdout='', query_result=''): 
    
    if "Critical" in query_result:
        code = 2
    elif "Warning" in query_result:
        code = 1
    else:
        code = 0
    stdout = query_result
    raise NagiosReturn(stdout, code)

class NagiosReturn(Exception):


    def __init__(self, message, code):
    
        self.message = message
        self.code = code

class MSSQLQuery(object):


    def __init__(self, query, options, stdout='', host='', modifier=1, *args, **kwargs):
        
        self.query = query
        self.stdout = stdout
        self.options = options
        self.host = host
        self.modifier = modifier

    def run_on_connection(self, connection):
 
        cur = connection.cursor()
        self.query = self.query.format(self.options.mode, self.options.warning, self.options.critical)

        cur.execute(self.query)
        self.query_result = cur.fetchone()[0]

    def finish(self):

        return_nagios(  self.options,
                        self.stdout,
                        self.query_result )

    def calculate_result(self):
    
        self.result = float(self.query_result) * self.modifier

    def do(self, connection):
   
        self.run_on_connection(connection)
        self.finish()

class MSSQLLOGSHIPQuery(MSSQLQuery):


    def __init__(self, *args, **kwargs):
        super(MSSQLLOGSHIPQuery, self).__init__(*args, **kwargs)

    def run_on_connection(self, connection):

        cur = connection.cursor()

        self.query = self.query.format(self.options.mode, self.options.primaryhost, self.options.primarydb, self.options.secondarydb)
        cur.execute(self.query)
        self.query_result = cur.fetchone()[0]

def parse_args():

    usage = "%prog -H <hostname> -U <username> -P <password>\n\
    [-p <port>] [-D <database>]\n\
    [--s <'storedproc'>] [-r <--result>] [-w <warn time>]\n\
    [-c <critical time>] [-h <help>] [-v <version>]"

    parser = OptionParser(usage=usage)

    required = OptionGroup(parser, "Required Options")
    required.add_option('-H' , '--hostname', help='Specify MSSQL Server Address', default=None)
    required.add_option('-U' , '--user', help='Specify MSSQL User Name', default=None)
    required.add_option('-P' , '--password', help='Specify MSSQL Password', default=None)
    parser.add_option_group(required)

    connection = OptionGroup(parser, "Optional Connection Information")
    connection.add_option('-p', '--port', help='Specify port.', default="1433")
    connection.add_option('-1', '--primaryhost', help='Specify primary host.', default=None)
    connection.add_option('-2', '--secondaryhost', help='Specify secondary host.', default=None)
    connection.add_option('-d', '--database', help='Specify database.', default=None)
    connection.add_option('-3', '--primarydb', help='Specify primary database.', default=None)
    connection.add_option('-4', '--secondarydb', help='Specify secondary database.', default=None)
    connection.add_option('-r', '--result', help='Specify expected_result.', default=None)
    connection.add_option('-s', '--storedproc', help='Specify storeproc.', default=None)
    connection.add_option('-t', '--type', help='Specify type of command.', default=None)
    parser.add_option_group(connection)

    nagios = OptionGroup(parser, "Nagios Plugin Information")
    nagios.add_option('-w', '--warning', help='Specify warning range.', default=None)
    nagios.add_option('-c', '--critical', help='Specify critical range.', default=None)
    parser.add_option_group(nagios)

    mode = OptionGroup(parser, "Mode Options")
    parser.add_option_group(mode)

    options, _ = parser.parse_args()

    if not options.user:
        parser.error('User is a required option.')
    if not options.password:
        parser.error('Password is a required option.')
    
    options.mode = options.storedproc 
    return options

def connect_db(options):
    
    host = options.hostname
    if options.type == 'logship':
    	host = options.secondaryhost
    if options.port:
        host += ":" + options.port
    start = time.time()
    try:
        mssql = pymssql.connect(host = host, user = options.user, password = options.password, database=options.database)
    except:
        print('ERROR - Failed to connect to {}'.format(host))
        sys.exit(2)
    total = time.time() - start
    return mssql, total, host

def main():

    options = parse_args()
    mssql, total, host = connect_db(options)
    
    execute_query(mssql, options, host)

def execute_query(mssql, options, host=''):
    
    sql_query = MODES[options.mode]
    sql_query['options'] = options
    sql_query['host'] = host
    options = vars(sql_query['options'])
    query_type = options['type']
    if query_type == 'logship':
        mssql_query = MSSQLLOGSHIPQuery(**sql_query)
    else:
        mssql_query = MSSQLQuery(**sql_query)
    mssql_query.do(mssql)

def run_tests(mssql, options, host):
    
    failed = 0
    total  = 0
    for mode in list(MODES.keys()):
        total += 1
        options.mode = mode
        try:
            execute_query(mssql, options, host)
        except NagiosReturn:
            print("{} passed!".format(mode))
        except Exception as e:
            failed += 1
            print("{} failed with: {}".format(mode, e))
    print('{}/{} tests failed.'.format(failed, total))

if __name__ == '__main__':
    try:
        main()
    except pymssql.OperationalError as e:
        print(e)
        sys.exit(3)
    except pymssql.InterfaceError as e:
        print(e)
        sys.exit(3)
    except IOError as e:
        print(e)
        sys.exit(3)
    except NagiosReturn as e:
        print(e.message)
        sys.exit(e.code)
    except Exception as e:
        print(type(e))
        print("Caught unexpected error. This could be caused by your sysperfinfo not containing the proper entries for this query, and you may delete this service check.")
        sys.exit(3)



    def __init__(self, *args, **kwargs):
        super(MSSQLLOGSHIPQuery, self).__init__(*args, **kwargs)

    def run_on_connection(self, connection):

        cur = connection.cursor()

        self.query = self.query.format(self.options.mode, self.options.primaryhost, self.options.primarydb, self.options.secondarydb)
        cur.execute(self.query)
        self.query_result = cur.fetchone()[0]

def parse_args():

    usage = "%prog -H <hostname> -U <username> -P <password>\n\
    [-p <port>] [-D <database>]\n\
    [--s <'storedproc'>] [-r <--result>] [-w <warn time>]\n\
    [-c <critical time>] [-h <help>] [-v <version>]"

    parser = OptionParser(usage=usage)

    required = OptionGroup(parser, "Required Options")
    required.add_option('-H' , '--hostname', help='Specify MSSQL Server Address', default=None)
    required.add_option('-U' , '--user', help='Specify MSSQL User Name', default=None)
    required.add_option('-P' , '--password', help='Specify MSSQL Password', default=None)
    parser.add_option_group(required)

    connection = OptionGroup(parser, "Optional Connection Information")
    connection.add_option('-p', '--port', help='Specify port.', default="1433")
    connection.add_option('-1', '--primaryhost', help='Specify primary host.', default=None)
    connection.add_option('-2', '--secondaryhost', help='Specify secondary host.', default=None)
    connection.add_option('-d', '--database', help='Specify database.', default=None)
    connection.add_option('-3', '--primarydb', help='Specify primary database.', default=None)
    connection.add_option('-4', '--secondarydb', help='Specify secondary database.', default=None)
    connection.add_option('-r', '--result', help='Specify expected_result.', default=None)
    connection.add_option('-s', '--storedproc', help='Specify storeproc.', default=None)
    connection.add_option('-t', '--type', help='Specify type of command.', default=None)
    parser.add_option_group(connection)

    nagios = OptionGroup(parser, "Nagios Plugin Information")
    nagios.add_option('-w', '--warning', help='Specify warning range.', default=None)
    nagios.add_option('-c', '--critical', help='Specify critical range.', default=None)
    parser.add_option_group(nagios)

    mode = OptionGroup(parser, "Mode Options")
    parser.add_option_group(mode)

    options, _ = parser.parse_args()

    if not options.user:
        parser.error('User is a required option.')
    if not options.password:
        parser.error('Password is a required option.')
    
    options.mode = options.storedproc 
    return options

def connect_db(options):
    
    host = options.hostname
    if options.type == 'logship':
    	host = options.secondaryhost
    if options.port:
        host += ":" + options.port
    start = time.time()
    mssql = pymssql.connect(host = host, user = options.user, password = options.password, database = options.database)
    total = time.time() - start
    return mssql, total, host

def main():

    options = parse_args()

    mssql, total, host = connect_db(options)
    
    execute_query(mssql, options, host)

def execute_query(mssql, options, host=''):
    
    sql_query = MODES[options.mode]
    sql_query['options'] = options
    sql_query['host'] = host
    if query_type == 'logship':
        mssql_query = MSSQLLOGSHIPQuery(**sql_query)
    else:
        mssql_query = MSSQLQuery(**sql_query)
    mssql_query.do(mssql)

def run_tests(mssql, options, host):
    
    failed = 0
    total  = 0
    for mode in list(MODES.keys()):
        total += 1
        options.mode = mode
        try:
            execute_query(mssql, options, host)
        except NagiosReturn:
            print("{} passed!".format(mode))
        except Exception as e:
            failed += 1
            print("{} failed with: {}".format(mode, e))
    print('{}/{} tests failed.'.format(failed, total))

if __name__ == '__main__':
    try:
        main()
    except pymssql.OperationalError as e:
        print('ERROR - {}'.format(e))
        sys.exit(2)
    except pymssql.InterfaceError as e:
        print('ERROR - {}'.format(e))
        sys.exit(2)
    except IOError as e:
        print('ERROR - {}'.format(e))
        sys.exit(2)
    except NagiosReturn as e:
        print(e.message)
        sys.exit(e.code)
    except Exception as e:
        print('ERROR - {}'.format(e))
        print(type(e))
        print('Caught unexpected error. This could be caused by your sysperfinfo not containing the proper entries for this query, and you may delete this service check.')
        sys.exit(3)

