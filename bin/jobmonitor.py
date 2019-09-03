#!/usr/bin/env python
"""
Module to look for jobs that may be hung in data transfer
"""

import argparse
import sys
import datetime
from copy import deepcopy
#import time

from despydb import desdbi


class Task(object):
    def __init__(self, id, name, start=None, duration=None, status=None, host=None, level=0, pfwid=None, operator=None):
        self.id = id
        self.name = name
        self.start = start
        self.duration = duration
        self.status = status
        self.children = []
        self.host = host
        self.depth = level
        self.pfwid = pfwid
        self.operator = operator

    def add_child(self, child):
        if child is None:
            return
        self.children.append(child)

    def __str__(self):
        line = ''
        if self.pfwid is not None:
            line += 'Pfw_attempt_id: %s     Operator: %s\n' % (self.pfwid, self.operator)
        base = " " * self.depth
        line += "%s%i  %30s   %27s   %23s   %5s   %30s\n" % (base, self.id, self.name, self.start, self.duration, str(self.status), self.host)
        for child in self.children:
            line += str(child)
        return line

    def is_running(self, dur):
        if isinstance(dur, (str)):
            dur = float(dur)
        if isinstance(dur, (int, float)):
            dur = datetime.timedelta(hours=dur)
        if self.duration < dur:
            return True
        result = False
        for child in self.children:
            result = result or child.is_running(dur)
        return result

    def __len__(self):
        tot = 1
        for child in self.children:
            tot += len(child)
        return tot

    def last(self):
        # does not do parallel properly
        if len(self.children) == 0:
            return self
        return self.children[0].last()

class Print(object):
    """ Class to capture printed output and stdout and reformat it to append
        the wrapper number to the lines

        Parameters
        ----------
        wrapnum : int
            The wrapper number to prepend to the lines

    """
    def __init__(self, fh):
        self.old_stdout = sys.stdout
        self.fh = fh

    def write(self, text):
        """ Method to capture, reformat, and write out the requested text

            Parameters
            ----------
            test : str
                The text to reformat

        """
        text = text.rstrip()
        self.fh.write('%s\n' % (text))
        self.old_stdout.write('%s\n' % (text))

    def close(self):
        """ Method to return stdout to its original handle

        """
        return self.old_stdout

    def flush(self):
        """ Method to force the buffer to flush

        """
        self.old_stdout.flush()

class Err(object):
    """ Class to capture printed output and stdout and reformat it to append
        the wrapper number to the lines

        Parameters
        ----------
        wrapnum : int
            The wrapper number to prepend to the lines
    """
    def __init__(self, fh):
        self.old_stderr = sys.stderr
        self.fh = fh

    def write(self, text):
        """ Method to capture, reformat, and write out the requested text

            Parameters
            ----------
            test : str
                The text to reformat
        """
        text = text.rstrip()
        self.fh.write('%s\n' % (text))
        self.old_stderr.write('%s\n' % (text))

    def close(self):
        """ Method to return stderr to its original handle

        """
        return self.old_stderr

    def flush(self):
        """ Method to force the buffer to flush

        """
        self.old_stderr.flush()


######################################################################
def parse_cmdline(argv):
    """ Parse the command line

        Parameters
        ----------
        argv : list
            The command line arguments

        Returns
        -------
        Dict containing the command line arguments
    """

    parser = argparse.ArgumentParser(description='Print information about probable hung attempts')
    parser.add_argument('--des_services', action='store', help='')
    parser.add_argument('--section', action='store',
                        help='Must be specified if not set in environment')
    parser.add_argument('--days', action='store', default=7, help='finished in last X days')
    parser.add_argument('--file', action='store')

    args = vars(parser.parse_args(argv))   # convert to dict

    return args

######################################################################
def query_attempts(dbh, pipeline, days):
    """ Method to get data about the current runs

        Parameters
        ----------
        dbh : DB handle
            Handle to the current DB connection
        pipeline : str
            The name of the pipeline to query for
        days : int
            The number of days to query over

        Returns
        -------
        Dict containing the attempt information
    """
    print "Querying pfw_attempt table for %s runs" % pipeline
    sql = "select r.pipeline, a.unitname,a.reqnum,a.attnum,a.task_id, a.archive_path, start_time, end_time, status, a.id as pfwid, a.operator as operator from pfw_request r, pfw_attempt a, task t where r.reqnum = a.reqnum and a.task_id = t.id and trunc(a.submittime) >= trunc(SYSTIMESTAMP) - %s and end_time is NULL and start_time is not NULL" % days
    if pipeline is not None:
        sql += " and r.pipeline='%s'" % pipeline
    curs = dbh.cursor()
    curs.execute(sql)
    desc = [d[0].lower() for d in curs.description]

    attinfo = {}
    for row in curs:
        dat = dict(zip(desc, row))
        attinfo[dat['task_id']] = dat
    print "  Number of attempts = %i" % len(attinfo)
    return attinfo

######################################################################
def save_att_taskids(attinfo, dbh):
    """ Method to store attempt id's in a gtt table

        Parameters
        ----------
        attinfo : dict
            Dict containing the attempt information
        dbh : DB handle
            Handle to the current DB connection
    """
    # successful jobs only (status=0)

    curs = dbh.cursor()
    sql = "delete from gtt_id"
    curs.execute(sql)

    # make list of lists to use as executemany params [ [tid1], [tid2], [tid3]... ]
    att_ids = []
    for task_id in attinfo.keys():
        att_ids.append([task_id])

    if len(att_ids) == 0:
        return
    # insert into the gtt_id table
    sql = "insert into gtt_id (id) values (%s)" % dbh.get_positional_bind_string(1)
    curs.executemany(sql, att_ids)

######################################################################
def query_tasks(dbh):
    """ Method to get the currently running tasks

        Parameters
        ----------
        dbh : DB handle

        Returns
        -------
        Tuple containing the task info (dict) and active transfers (dict)
    """
    # assumes attempt task_ids are in gtt_id table

    curs = dbh.cursor()

    # query the task table
    sql = "select t.label,t.exec_host,t.name,t.status,t.id,t.parent_task_id,t.start_time,t.root_task_id, SYSTIMESTAMP-t.start_time as length from task t, gtt_id g where t.root_task_id=g.id and (end_time is null or status!=0)"
    curs.execute(sql)
    desc = [d[0].lower() for d in curs.description]

    results = {}
    for row in curs:
        dat = dict(zip(desc, row))
        if dat['root_task_id'] not in results:
            results[dat['root_task_id']] = {}
        results[dat['root_task_id']][dat['id']] = dat

    sql = "select s.request_time,s.grant_time,s.release_time, t.exec_host,t.name,t.id,t.root_task_id,t.parent_task_id,t.start_time from seminfo s, task t, gtt_id g where t.root_task_id=g.id and t.name like 'trans%' and t.id=s.task_id and t.end_time is NULL"
    curs.execute(sql)
    desc = [dd[0].lower() for dd in curs.description]

    results_trans = {}
    for row in curs:
        dat = dict(zip(desc, row))
        if dat['root_task_id'] not in results_trans:
            results_trans[dat['root_task_id']] = {}
        results_trans[dat['root_task_id']][dat['id']] = dat

    return results, results_trans

######################################################################
def connect(tree, taskinfo):
    """ Method to connect related tasks together. Recursively goes through the input.

        Parameters
        ----------
        tree : dict
            Empty dictionary that will contain the connected tasks with the root task at the top
        taskinfo : dict
            Dictionary containing the task info
    """
    delinfo = []
    for node in tree.keys():
        for task, val in taskinfo.iteritems():
            if val['parent_task_id'] == node:
                tree[node][val['id']] = {}
                delinfo.append(task)
        for task in delinfo:
            del taskinfo[task]
        delinfo = []
        connect(tree[node], taskinfo)

######################################################################
def make_tree(taskinfo):
    """ Method to find the root_task_id's and then generate the full task trees

        Parameters
        ----------
        taskinfo : dict
            Dintionary containing the task info

        Returns
        -------
        Dict containing the tree(s) of tasks, with the root_task(s) as the top
    """
    tree = {}
    tasks = deepcopy(taskinfo)
    for task, val in tasks.iteritems():
        if val['parent_task_id'] is None:
            rootid = val['id']
            del tasks[task]
            break
    tree[rootid] = {}
    connect(tree, tasks)
    return tree

######################################################################
def write_tree(tree, tasks, level=0, pfwid=None, operator=None):
    """ Method to write the task trees to thml. Recursively calls itself to construct the output

        Parameters
        ----------
        tree : dict
            Dictionary containing the task trees
        tasks : dict
            Dictionary containing the task data
        level : int
            Int to indicate the level of indentation (defualt is 0, no indentation)

        Returns
        -------
        Tuple containing the running host for the tasks, the current status,
        and html representation of the task
    """
    dtm = datetime.timedelta(hours=4)
    #base = " " * level
    line = None
    host = None
    status = 0
    for tid, val in tree.iteritems():
        if level == 0 and tasks[tid]['length'] < dtm:
            return None, None, None
        if level == 0 and tasks[tid]['status'] is not None:
            return None, None, None
        host = tasks[tid]['exec_host']
        if 'exec' in tasks[tid]['name']:
            execl = "%s (%s)" % (tasks[tid]['name'], tasks[tid]['label'])
        else:
            execl = tasks[tid]['name']
        if tasks[tid]['length'] is not None:
            if tasks[tid]['length'] > dtm and tasks[tid]['name'] not in ['attempt', 'block', 'job']:
                status = 1
        if tasks[tid]['status'] is not None:
            if int(tasks[tid]['status']) != 0:
                status = 2
#        line.append("%s%i  %30s   %27s   %23s   %5s" % (base, tasks[tid]['id'], execl, tasks[tid]['start_time'], tasks[tid]['length'], str(tasks[tid]['status'])))
        line = Task(tasks[tid]['id'], execl, tasks[tid]['start_time'], tasks[tid]['length'], tasks[tid]['status'], tasks[tid]['exec_host'], level, pfwid, operator)
        temp, stat, lns = write_tree(val, tasks, level+1)
        if temp is not None:
            host = temp
        line.add_child(lns)
        status = max(stat, status)
    return host, status, line

def header(head):
    print'\n'
    print '=================================================================================================================================='
    print head + '\n'
    print "Task id                      Name               Start time                    Duration               Status        Host"
    print "----------------------------------------------------------------------------------------------------------------------------------"


######################################################################
def find_hung(attinfo, taskinfo):
    """ Method to find potentail hung jobs

        Parameters
        ----------
        attinfo : dict
            Dictionary containing attempt information
        taskinfo : dict
            Dictionary containing the task information
    """
    lon = []
    intro = False
    possible_trouble = []
    to_kill = []
    for attd in sorted(attinfo.values(), key=lambda x: x['start_time']):
        atid = attd['task_id']
        tree = make_tree(taskinfo[atid])
        host, status, lines = write_tree(tree, taskinfo[atid], pfwid=attd['pfwid'], operator=attd['operator'])
        if lines is None:
            continue
        if len(lines) < 3:
            to_kill.append(lines)
            continue
        if len(lines) == 3:
            if lines.last().name == 'job':
                to_kill.append(lines)
                continue
            if lines.last().name in ('begblock','blockpost'):
                possible_trouble.append(lines)
                continue
        if lines.is_running(1):
            lon.append(lines)
            continue
        if not intro:
            header("The following runs are taking longer than expected, and may be stuck")
            intro = True
        print lines
        print ''

    if not intro:
        print "\nNo hung jobs detected."
    if len(lon) > 0:
        header("The following are running longer than expected, but appear to still be active")
        for l in lon:
            print l
    if len(possible_trouble) > 0:
        header("The following may be stuck in PFW processes.")
        for l in possible_trouble:
            print l
    if len(to_kill) > 0:
        header("The following appear to have stopped early and may need to be killed")
        for l in to_kill:
            print l

    print ''

def h2(text):
    print '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
    print text
    print "Task id    Task name    Exec host    Start time    Request time    Grant time    Release Time"    
######################################################################
def find_trans_hung(attinfo, taskinfo):
    """ Method to find jobs potentailly hung in file transfer

        Parameters
        ----------
        attinfo : dict
            Dictionary containing attempt information
        taskinfo : dict
            Dictionary containing the task information
    """

    intro = False

    reptime = datetime.datetime.now()
    count = 0
    for attd in sorted(attinfo.values(), key=lambda x: x['start_time']):
        atid = attd['task_id']
        if atid in taskinfo:
            found = False
            for tdict in sorted(taskinfo[atid].values(), key=lambda x: x['start_time']):
                if (tdict['name'].startswith('trans_input') and (reptime - tdict['start_time']).total_seconds() > 2*60*60) or (tdict['name'].startswith('trans_output') and (reptime - tdict['start_time']).total_seconds() > 10*60*60):
                    found = True
            if not found:
                break
            if not intro:
                 h2("The following transfers may be hung\n")
                 intro = True
            print "  %s_r%dp%02d  %s  %s  %i\n" % (attd['unitname'], int(attd['reqnum']), int(attd['attnum']), attd['pfwid'], attd['archive_path'], atid)
            for tdict in sorted(taskinfo[atid].values(), key=lambda x: x['start_time']):
                if (tdict['name'].startswith('trans_input') and (reptime - tdict['start_time']).total_seconds() > 2*60*60) or (tdict['name'].startswith('trans_output') and (reptime - tdict['start_time']).total_seconds() > 10*60*60):
                    print " %s  %s  %s  %s  %s  %s  %s" % (tdict['id'], tdict['name'], tdict['exec_host'], tdict['start_time'], tdict['request_time'], tdict['grant_time'], tdict['release_time'])
                    count += 1
    if count == 0:
        print "\nNo hung transfers found.\n"

######################################################################
def main(argv):
    """ Main entry point

        Parameters
        ----------
        argv : list
            Command line arguments
    """
    args = parse_cmdline(argv)
    dbh = desdbi.DesDbi(args['des_services'], args['section'])
    curs = dbh.cursor()
    #curs.execute("select distinct(pipeline) from pfw_request where pipeline != 'hostname'")
    #results = curs.fetchall()
    #for res in results:
    pipeline = 'finalcut'
    stdp = None
    if args['file'] is not None:
        fh = open(args['file'], 'w', buffering=0)
        fh.write('<html><body><pre>\n')
        stdp = Print(fh)
        stde = Err(fh)
        sys.stdout = stdp
        sys.stderr = stde
    attinfo = query_attempts(dbh, pipeline, args['days'])
    save_att_taskids(attinfo, dbh)
    taskinfo, trans_taskinfo = query_tasks(dbh)
    find_hung(attinfo, taskinfo)
    find_trans_hung(attinfo, trans_taskinfo)
    if stdp is not None:
        fh.write('</pre></body></html>')
        fh.close()
        stdp.close()
        stde.close()


if __name__ == '__main__':
    main(sys.argv[1:])
