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
    parser.add_argument('--days', action='store', default=3, help='finished in last X days')
    parser.add_argument('--file', action='store')

    args = vars(parser.parse_args(argv))   # convert to dict

    return args

######################################################################
def query_attempts(dbh, reqnum):
    """ Method to get data about the current runs

        Parameters
        ----------
        dbh : DB handle
            Handle to the current DB connection
        pipeline : str
            The name of the pipeline to query for
        days : int
            The number of days to query over
        html : file handle
            File handle to the output html file.

        Returns
        -------
        Dict containing the attempt information
    """
    #print "Querying pfw_attempt table for %s runs" % pipeline
    sql = "select a.is,a.task_id from pfw_attempt a where a.reqnum=%i" % reqnum
    #if pipeline is not None:
    #    sql += " and r.pipeline='%s'" % pipeline
    curs = dbh.cursor()
    #a = time.time()
    #print sql
    curs.execute(sql)
    desc = [d[0].lower() for d in curs.description]
    #print desc

    attinfo = {}
    for row in curs:
        dat = dict(zip(desc, row))
        #print d
        attinfo[dat['task_id']] = dat
    #print "TIME1 ", time.time() - a
    #html.write("  Number of attempts = %i" % len(attinfo))
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
    #print len(att_ids)
    #curs.execute("select count(*) from gtt_id")
    #res= curs.fetchall()
    #print res

    if len(att_ids) == 0:
        #print "returning"
        return
    # insert into the gtt_id table
    sql = "insert into gtt_id (id) values (%s)" % dbh.get_positional_bind_string(1)
    curs.executemany(sql, att_ids)

    #curs.execute("select count(*) from gtt_id")
    #res = curs.fetchall()
    #print res
    #print len(res)

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
    sql = "select t.label,t.name,t.id,t.parent_task_id,t.start_time,t.end_time,t.root_task_id, t.end_time-t.start_time as length from task t, gtt_id g where t.root_task_id=g.id and end_time is not null"
    curs.execute(sql)
    desc = [d[0].lower() for d in curs.description]

    results = {}
    for row in curs:
        dat = dict(zip(desc, row))
        if dat['root_task_id'] not in results:
            results[dat['root_task_id']] = {}
        results[dat['root_task_id']][dat['id']] = dat

    sql = "select s.request_time,s.grant_time,s.release_time, ,t.name,t.id,t.root_task_id,t.parent_task_id,t.start_time from seminfo s, task t, gtt_id g where t.root_task_id=g.id and t.name like 'trans%' and t.id=s.task_id and t.end_time is not NULL"
    #a = time.time()
    #print "START3"
    curs.execute(sql)
    #print "TIME3 ", time.time() - a
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
def write_tree(tree, tasks, level=0):
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
    #ddt = datetime.timedelta(hours=24)
    base = "&nbsp;&nbsp;&nbsp;" * level
    line = ""
    host = None
    status = 0
    for tid, val in tree.iteritems():
        #print "T",tid, tasks[tid]['id']
        host = tasks[tid]['exec_host']
        if 'exec' in tasks[tid]['name']:
            execl = "%s (%s)" % (tasks[tid]['name'], tasks[tid]['label'])
        else:
            execl = tasks[tid]['name']
        clr = '"#FFFFFF"'
        if tasks[tid]['length'] is not None:
            if tasks[tid]['length'] > dtm and tasks[tid]['name'] not in ['attempt', 'block', 'job']:
                clr = '"#FFFF00"'
                status = 1
            #elif tasks[tid]['length'] > ddt and tasks[tid]['name'] not in ['attempt', 'block', 'job']:
            #    clr = '"#FF0000"'
        cclr = '"#FFFFFF"'
        if tasks[tid]['status'] is not None:
            if int(tasks[tid]['status']) != 0:
                cclr = '"#FF0000"'
                status = 2
        line += "<tr><td>%s%i</td><td><b>%s</b></td><td>%s</td><td bgcolor=%s>%s</td><td bgcolor=%s>%s</td>\n" % (base, tasks[tid]['id'], execl, tasks[tid]['start_time'], clr, tasks[tid]['length'], cclr, str(tasks[tid]['status']))
        temp, stat, lns = write_tree(val, tasks, level+1)
        if temp is not None:
            host = temp
        line += lns
        status = max(stat, status)
    return host, status, line

######################################################################
def find_hung(attinfo, taskinfo, html):
    """ Method to find potentail hung jobs

        Parameters
        ----------
        attinfo : dict
            Dictionary containing attempt information
        taskinfo : dict
            Dictionary containing the task information
        html : file handle
            File hand for html output
    """
    for attd in sorted(attinfo.values(), key=lambda x: x['start_time']):
        atid = attd['task_id']
        tree = make_tree(taskinfo[atid])
        html.write('<table border=0>\n')
        host, status, lines = write_tree(tree, taskinfo[atid])
        clr = '"#FFFFFF"'
        if status == 1:
            clr = '"#FFFF00"'
        elif status == 2:
            clr = '"#FF0000"'
        html.write("<tr><td bgcolor=%s><b><a href='#' onclick=\"setTable('T%s');return false\">%s_r%dp%02d</a></b>&nbsp;&nbsp;&nbsp;&nbsp;%s&nbsp;&nbsp;&nbsp;&nbsp;%s&nbsp;&nbsp;&nbsp;&nbsp;%s</td></tr>" % (clr, atid, attd['unitname'], int(attd['reqnum']), int(attd['attnum']), attd['start_time'], attd['archive_path'], host))
        html.write("<tr><td><table border=1 ID='T%s' STYLE='display:none;'>\n" % atid)
        html.write("<tr><th>Taskid</th><th>Name</th><th>Start time</th><th>Duration</th><th>Status</th></tr>\n")
        html.write(lines)
        html.write("</table></td></tr></table>\n")

    html.write("<P>\n")
######################################################################
def find_trans_hung(attinfo, taskinfo, html):
    """ Method to find jobs potentailly hung in file transfer

        Parameters
        ----------
        attinfo : dict
            Dictionary containing attempt information
        taskinfo : dict
            Dictionary containing the task information
        html : file handle
            File hand for html output
    """

    reptime = datetime.datetime.now()
    html.write("<h3>Hung transfers</h3>")
    count = 0
    for attd in sorted(attinfo.values(), key=lambda x: x['start_time']):
        #tasks_no_end = {}
        #tasks_failed = {}
        atid = attd['task_id']
        if atid in taskinfo:
            found = False
            for tdict in sorted(taskinfo[atid].values(), key=lambda x: x['start_time']):
                if (tdict['name'].startswith('trans_input') and (reptime - tdict['start_time']).total_seconds() > 2*60*60) or (tdict['name'].startswith('trans_output') and (reptime - tdict['start_time']).total_seconds() > 10*60*60):
                    found = True
            if not found:
                break
            html.write("&nbsp;<b>%s_r%dp%02d</b>&nbsp;&nbsp;%s&nbsp;&nbsp;%i\n" % (attd['unitname'], int(attd['reqnum']), int(attd['attnum']), attd['archive_path'], atid))
            html.write("<table border=1>\n")
            html.write("<tr><th>Task id</th><th>Task name</th><th>Exec host</th><th>Start time</th><th>Request time</th><th>Grant time</th><th>Release time</th></tr>\n")
            for tdict in sorted(taskinfo[atid].values(), key=lambda x: x['start_time']):
                if (tdict['name'].startswith('trans_input') and (reptime - tdict['start_time']).total_seconds() > 2*60*60) or (tdict['name'].startswith('trans_output') and (reptime - tdict['start_time']).total_seconds() > 10*60*60):
                    clr = "\"#FF0000\""
                    html.write("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td bgcolor=%s>%s</td></tr>\n" % (tdict['id'], tdict['name'], tdict['exec_host'], tdict['start_time'], tdict['request_time'], tdict['grant_time'], clr, tdict['release_time']))
                    count += 1
            html.write("</table>\n")
    if count == 0:
        html.write("&nbsp;&nbsp;No hung transfers found.")

######################################################################
def main(argv):
    """ Main entry point

        Parameters
        ----------
        argv : list
            Command line arguments
    """
    args = parse_cmdline(argv)
    html = open(args['file'], 'w')
    html.write("<html>\n<head>\n<title>Pipeline Status Monitor</title>\n")
    html.write('<script type="text/javascript">//<![CDATA[\n')
    html.write('function setTable(what) {\n')
    html.write('if(document.getElementById(what).style.display=="none") {\n')
    html.write('  document.getElementById(what).style.display="block";\n')
    html.write('}\n')
    html.write('else if(document.getElementById(what).style.display=="block") {\n')
    html.write('  document.getElementById(what).style.display="none";\n')
    html.write('}\n')
    html.write('}\n')
    html.write('//]]></script>\n')
    html.write("</head>\n<body>\n")
    dbh = desdbi.DesDbi(args['des_services'], args['section'])
    curs = dbh.cursor()
    curs.execute("select distinct(pipeline) from pfw_request where pipeline != 'hostname'")
    results = curs.fetchall()
    for res in results:
        pipeline = res[0]
        html.write("<h2>%s</h2>\n" % pipeline)
        attinfo = query_attempts(dbh, pipeline, args['days'], html)
        save_att_taskids(attinfo, dbh)
        taskinfo, trans_taskinfo = query_tasks(dbh)
        find_hung(attinfo, taskinfo, html)
        find_trans_hung(attinfo, trans_taskinfo, html)
    html.write("</body></html>\n")
    html.close()


if __name__ == '__main__':
    main(sys.argv[1:])
