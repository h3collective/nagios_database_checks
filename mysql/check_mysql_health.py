#!/usr/bin/env python3

########################################################################
# check_mysql_server - A Nagios plugin to check Microsoft SQL Server
# Copyright (C) 2017 Nagios Enterprises
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
################### check_mysql_server.py ##############################
# Version    : 2.1.1
# Date       : 11/2019
# Maintainer : Matthew Paczosa
# License    : GPLv2 (LICENSE.md / https://www.gnu.org/licenses/old-licenses/gpl-2.0.html)
########################################################################

import pymysql
import time
import sys
import tempfile
import traceback
try:
    import cPickle as pickle
except:
    import pickle
from optparse import OptionParser, OptionGroup

BASE_QUERY = "SELECT cntr_value FROM sysperfinfo WHERE counter_name='{}' AND instance_name='';"
INST_QUERY = "SELECT cntr_value FROM sysperfinfo WHERE counter_name='{}' AND instance_name='{}';"
OBJE_QUERY = "SELECT cntr_value FROM sysperfinfo WHERE counter_name='{}';"
SLAVE_QUERY = "SHOW SLAVE STATUS"
DIVI_QUERY = "SELECT cntr_value FROM sysperfinfo WHERE counter_name LIKE '{}%' AND instance_name='{}';"
CON_QUERY = "SHOW /*!50000 global */ STATUS LIKE 'Threads_connected'"
MEM_QUERY = "SELECT 100*(1.0-(available_physical_memory_kb/(total_physical_memory_kb*1.0))) FROM sys.dm_os_sys_memory;" 
CPU_QUERY = "SELECT "\
    "record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS [CPU] "\
    "FROM ( "\
        "SELECT[timestamp], CONVERT(XML, record) AS [record] "\
            "FROM sys.dm_os_ring_buffers WITH ( NOLOCK ) "\
            "WHERE ring_buffer_type=N'RING_BUFFER_SCHEDULER_MONITOR' "\
            "AND record LIKE N'%<SystemHealth>%'"\
    ") as x;"

MODES = {

    'connections'       : { 'help'      : 'Number of users connected',
                            'stdout'    : 'Number of users connected is {}',
                            'label'     : 'connections',
                            'type'      : 'standard',
                            'query'     : CON_QUERY 
                            },

    'memory'            : { 'help'      : 'Used server memory',
                            'stdout'    : 'Server using {}% of memory',
                            'label'     : 'memory',
                            'query'     : MEM_QUERY
                            },

    'cpu'               : { 'help'      : 'Server CPU utilization',
                            'stdout'    : 'Current CPU utilization is {}%',
                            'label'    : 'cpu',
                            'query'     : CPU_QUERY
                            },

    'slave'		: { 'help'      : 'Page Life Expectancy',
                            'query'     : SLAVE_QUERY,
			    'stdout'	: '{} file {} {}/{}',
                            'type'      : 'slave',
			    'modifier'  : '100'
                            },
    
    'slavelag'		: { 'help'      : 'Page Life Expectancy',
			    'stdout'	: 'SLAVE is {} seconds behind',
			    'label'	: 'lag',
                            'query'     : SLAVE_QUERY,
                            'type'      : 'lag',
                            },
   
    #~ 'debug'             : { 'help'      : 'Used as a debugging tool.',
                            #~ 'stdout'    : 'Debugging: ',
                            #~ 'label'     : 'debug',
                            #~ 'query'     : DIVI_QUERY.format('Average Wait Time', '_Total'),
                            #~ 'type'      : 'divide' 
                            #~ },
    
    'time2connect'      : { 'help'      : 'Time to connect to the database.' },
    
    'test'              : { 'help'      : 'Run tests of all queries against the database.' },

}

def return_nagios(options, stdout='', result='', unit='', label=''):

    if type(result) is not tuple:
        if is_within_range(options.critical, result):
            prefix = 'CRITICAL: '
            code = 2
        elif is_within_range(options.warning, result):
            prefix = 'WARNING: '
            code = 1
        else:
            prefix = 'OK: '
            code = 0
        strresult = str(result)
        try:
            stdout = stdout.format(strresult)
        except TypeError as e:
            pass
        stdout = '{}{}| {}={}{};{};{};;'.format(prefix, stdout, label, strresult, unit, options.warning or '', options.critical or '')
        raise NagiosReturn(stdout, code)
    else:
        if result[0] and result [1] == 'Yes':
            status = 'OK:'
            code = 0
        else: 
            status = 'CRITICAL:'
            code = 0
        stdout = stdout.format(status, result[2], result[3], result[4])
        raise NagiosReturn(stdout, code)

class NagiosReturn(Exception):

    
    def __init__(self, message, code):

        self.message = message
        self.code = code

class MYSQLQuery(object):

    
    def __init__(self,type, query, options, label='', unit='', stdout='', host='', modifier=1, *args, **kwargs):
        
        self.type = type
        self.query = query
        self.label = label
        self.unit = unit
        self.stdout = stdout
        self.options = options
        self.host = host
        self.modifier = modifier
    
    def run_on_connection(self, connection):

        cur = connection.cursor(pymysql.cursors.DictCursor)
        cur.execute(self.query)
        self.query_result = cur.fetchone()['Value']

    def finish(self):

        return_nagios(  self.options,
                        self.stdout,
                        self.result,
                        self.unit,
                        self.label )
	 
    def calculate_result(self):

        self.result = float(self.query_result) * self.modifier
    
    def do(self, connection):

        self.run_on_connection(connection)
        self.calculate_result()
        self.finish()

class MYSQLDivideQuery(MYSQLQuery):

    
    def __init__(self, *args, **kwargs):

        super(MYSQLDivideQuery, self).__init__(*args, **kwargs)
    
    def calculate_result(self):

        if self.query_result[1] != 0:
            self.result = (float(self.query_result[0]) / self.query_result[1]) * self.modifier
        else:
            self.result = float(self.query_result[0]) * self.modifier
    
    def run_on_connection(self, connection):

        cur = connection.cursor()
        cur.execute(self.query)
        self.query_result = [x[0] for x in cur.fetchall()]

class MYSQLDeltaQuery(MYSQLQuery):

    
    def make_pickle_name(self):

        tmpdir = tempfile.gettempdir()
        tmpname = hash(self.host + self.query)
        self.picklename = '{}/mysql-{}.tmp'.format(tmpdir, tmpname)
    
    def calculate_result(self):

        self.make_pickle_name()
        
        try:
            tmpfile = open(self.picklename)
        except IOError:
            tmpfile = open(self.picklename, 'w')
            tmpfile.close()
            tmpfile = open(self.picklename)
        try:
            try:
                last_run = pickle.load(tmpfile)
            except EOFError as ValueError:
                last_run = { 'time' : None, 'value' : None }
        finally:
            tmpfile.close()
        
        if last_run['time']:
            old_time = last_run['time']
            new_time = time.time()
            old_val  = last_run['query_result']
            new_val  = self.query_result
            self.result = ((new_val - old_val) / (new_time - old_time)) * self.modifier
        else:
            self.result = None
        
        new_run = { 'time' : time.time(), 'query_result' : self.query_result }
        
        #~ Will throw IOError, leaving it to acquiesce
        tmpfile = open(self.picklename, 'w')
        pickle.dump(new_run, tmpfile)
        tmpfile.close()

class MYSQLSlaveQuery(MYSQLQuery) :


    def run_on_connection(self, connection):

        cur = connection.cursor(pymysql.cursors.DictCursor)
        cur.execute(self.query)

        self.query_result = cur.fetchone()

    def calculate_result(self):

        self.result =  self.query_result['Slave_IO_Running'],self.query_result['Slave_SQL_Running'],self.query_result['Relay_Master_Log_File'], self.query_result['Read_Master_Log_Pos'], self.query_result['Exec_Master_Log_Pos']

class MYSQLSlaveLagQuery(MYSQLQuery) :


    def run_on_connection(self, connection):

        cur = connection.cursor(pymysql.cursors.DictCursor)
        cur.execute(self.query)
        self.query_result = cur.fetchone()

    def calculate_result(self):

        self.result = float(self.query_result['Seconds_Behind_Master']) * self.modifier

def parse_args():
    
    usage = "usage: %prog -H hostname -U user -P password -T table --m mode"
    parser = OptionParser(usage=usage)
    
    required = OptionGroup(parser, "Required Options")
    required.add_option('-H' , '--hostname', help='Specify MYSQL Server Address', default=None)
    required.add_option('-U' , '--user', help='Specify MYSQL User Name', default=None)
    required.add_option('-P' , '--password', help='Specify MYSQL Password', default=None)
    parser.add_option_group(required)
    
    connection = OptionGroup(parser, "Optional Connection Information")
    connection.add_option('-I', '--instance', help='Specify instance', default=None)
    connection.add_option('-p', '--port', help='Specify port.', default=None)
    connection.add_option('-m', '--mode', help='specify mode', default=None)
   
    parser.add_option_group(connection)
    
    nagios = OptionGroup(parser, "Nagios Plugin Information")
    nagios.add_option('-w', '--warning', help='Specify warning range.', default=None)
    nagios.add_option('-c', '--critical', help='Specify critical range.', default=None)
    parser.add_option_group(nagios)
 
    mode = OptionGroup(parser, "Mode Options")
    options, _ = parser.parse_args()
 
    if not options.hostname:
        parser.error('Hostname is a required option.')
    if not options.user:
        parser.error('User is a required option.')
    if not options.password:
        parser.error('Password is a required option.')
    
    if options.instance and options.port:
        parser.error('Cannot specify both instance and port.')
    
    return options

def is_within_range(nagstring, value):

    if not nagstring:
        return False
    import re
    import operator
    first_float = r'(?P<first>(-?[0-9]+(\.[0-9]+)?))'
    second_float= r'(?P<second>(-?[0-9]+(\.[0-9]+)?))'
    actions = [ (r'^{}$'.format(first_float),lambda y: (value > float(y.group('first'))) or (value < 0)),
                (r'^{}:$'.format(first_float),lambda y: value < float(y.group('first'))),
                (r'^~:{}$'.format(first_float),lambda y: value > float(y.group('first'))),
                (r'^{}:{}$'.format(first_float,second_float), lambda y: (value < float(y.group('first'))) or (value > float(y.group('second')))),
                (r'^@{}:{}$'.format(first_float,second_float), lambda y: not((value < float(y.group('first'))) or (value > float(y.group('second')))))]
    for regstr,func in actions:
        res = re.match(regstr,nagstring)
        if res: 
            return func(res)
    raise Exception('Improper warning/critical format.')

def connect_db(options):

    host = options.hostname
    if options.instance:
        host += "\\" + options.instance
    elif options.port:
        host += ":" + options.port
    start = time.time()
    try:
        mysql = pymysql.connect(host = host, user = options.user, password = options.password)
    except:
        print('Failed to connect to {}'.forma(host))
        sys.exit(2)

    total = time.time() - start
    return mysql, total, host

def main():

    options = parse_args()
    mysql, total, host = connect_db(options) 
    if options.mode =='test':
        run_tests(mysql, options, host)
        
    elif not options.mode or options.mode == 'time2connect':
        return_nagios(  options,
                        stdout='Time to connect was {}s',
                        label='time',
                        unit='s',
                        result=total )
                        
    else:
        execute_query(mysql, options, host)

def execute_query(mysql, options, host=''):

    sql_query = MODES[options.mode]
    sql_query['options'] = options
    sql_query['host'] = host
    query_type = sql_query.get('type')
    if query_type == 'delta':
        mysql_query = MYSQLDeltaQuery(**sql_query)
    elif query_type == 'divide':
        mysql_query = MYSQLDivideQuery(**sql_query)
    elif query_type == 'lag':
        mysql_query = MYSQLSlaveLagQuery(**sql_query) 
    elif query_type == 'slave': 
        mysql_query = MYSQLSlaveQuery(**sql_query)
    else:
        mysql_query = MYSQLQuery(**sql_query)
    mysql_query.do(mysql)

def run_tests(mysql, options, host):

    failed = 0
    total  = 0
    del MODES['time2connect']
    del MODES['test']
    for mode in list(MODES.keys()):
        total += 1
        options.mode = mode
        try:
            execute_query(mysql, options, host)
        except NagiosReturn:
            print('{} passed!'.format(mode))
        except Exception as e:
            failed += 1
            print('{} failed with: {}'.format(mode, e))
    print('{}/{} tests failed.'.format(failed, total))
    
if __name__ == '__main__':

       try:
        main()
    except pymysql.OperationalError as e:
        print('ERROR - {}'.format(e))
        sys.exit(2)
    except pymysql.InterfaceError as e:
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
