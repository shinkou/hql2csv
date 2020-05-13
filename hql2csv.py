#!/usr/bin/env python3
# vim: fenc=utf-8 ff=unix lcs=tab\:>. list noet sw=4 ts=4 tw=0
import argparse, csv, time
from pyhive import hive
from TCLIService.ttypes import TOperationState


TOpsStates = {
	TOperationState.INITIALIZED_STATE: "Initialized"
	, TOperationState.RUNNING_STATE: "Running"
	, TOperationState.FINISHED_STATE: "Finished"
	, TOperationState.CANCELED_STATE: "Cancelled"
	, TOperationState.CLOSED_STATE: "Closed"
	, TOperationState.ERROR_STATE: "Error"
	, TOperationState.PENDING_STATE: "Pending"
	, TOperationState.TIMEDOUT_STATE: "TimedOut"
}


def get_args():
	'''
	Get command line options and arguments
	'''
	parser = argparse.ArgumentParser(description = 'Hive Query Load 2 CSV')
	parser.add_argument(
		'queries'
		, metavar = 'QUERY'
		, type = str
		, nargs = '+'
		, help = 'HQL statement of the query'
	)
	parser.add_argument(
		'-o'
		, '--output'
		, action = 'store'
		, type = str
		, default = 'output.csv'
		, help = 'filepath of the output CSV (default: "output.csv")'
	)
	parser.add_argument(
		'-H'
		, '--host'
		, action = 'store'
		, type = str
		, default = 'localhost'
		, help = 'host of the endpoint (default: "localhost")'
	)
	parser.add_argument(
		'-P'
		, '--port'
		, action = 'store'
		, type = int
		, default = 10000
		, help = 'port of the endpoint (default: 10000)'
	)
	parser.add_argument(
		'-D'
		, '--database'
		, action = 'store'
		, type = str
		, default = 'default'
		, help = 'database to connect to (default: "default")'
	)
	parser.add_argument(
		'-u'
		, '--username'
		, action = 'store'
		, type = str
		, help = 'name of the connection user (optional)'
	)
	parser.add_argument(
		'-p'
		, '--password'
		, action = 'store'
		, type = str
		, help = 'connection password (optional)'
	)
	parser.add_argument(
		'-A'
		, '--auth'
		, action = 'store'
		, type = str
		, help = 'authentication method, "LDAP" or "CUSTOM" only (optional)'
	)
	parser.add_argument(
		'--poll'
		, action = 'store'
		, type = int
		, default = 5
		, help = 'status polling interval in seconds (default: 5)'
	)
	return parser.parse_args()


def statusline(s):
	'''
	Output string as status line which is static on the screen
	'''
	print("\r\x1b[2K%s" % (s), end = '', flush = True)


def hql2csv(args):
	'''
	Process queries and output results
	'''
	blue = lambda s: '\x1b[1;34m%s\x1b[0m' % (s)
	brown = lambda s: '\x1b[1;33m%s\x1b[0m' % (s)
	green = lambda s: '\x1b[1;32m%s\x1b[0m' % (s)
	purple = lambda s: '\x1b[1;35m%s\x1b[0m' % (s)
	red = lambda s: '\x1b[1;31m%s\x1b[0m' % (s)
	white = lambda s: '\x1b[1;37m%s\x1b[0m' % (s)
	elapse = lambda t: statusline('%s: %ds' % (blue('Elapsed time'), t))
	cnxinfo = vars(args)
	cnxinfo = {k: cnxinfo[k] for k in ('host', 'port', 'database', 'username', 'password', 'auth') if k in cnxinfo and cnxinfo[k] is not None}
	print(white(cnxinfo))
	con =  hive.connect(**cnxinfo)
	cursor = con.cursor()
	with open(args.output, 'w') as f:
		writer = csv.writer(f)
		for query in args.queries:
			start_time = time.time()
			print('%s "%s"... ' % (brown('Sending query'), query), end = '', flush = True)
			cursor.execute(query, async_ = True)
			print(green('OK!'))
			elapse(time.time() - start_time)
			status = cursor.poll().operationState
			while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
				time.sleep(args.poll)
				elapse(time.time() - start_time)
				status = cursor.poll().operationState
			print()
			print('%s: %s' % (purple('Query status'), white(TOpsStates[status]) if status in TOpsStates else red('Unknown')))
			r = cursor.fetchone()
			while r is not None:
				writer.writerow(r)
				r = cursor.fetchone()
			print(green('Done!'))
	con.close()


if '__main__' == __name__:
	hql2csv(get_args())
