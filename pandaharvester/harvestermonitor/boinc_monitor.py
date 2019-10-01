import os.path
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils

from pandaharvester.boinc.submit_api import *
from pandaharvester.boinc.boinc_config import *

# logger
baseLogger = core_utils.setup_logger('boinc_monitor')


# dummy monitor
class BoincMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    def check_batch(self, batch_id):
  
        req = REQUEST()
        req.project = boinc_project['url']
        req.authenticator = boinc_project['authenticator']

        req.batch_id = batch_id
        req.get_cpu_time = True
        req.get_job_details = True
        r = query_batch(req)
        return r

    # check workers
    def check_workers(self, workspec_list):
        """Check status of workers. This method takes a list of WorkSpecs as input argument
        and returns a list of worker's statuses.
        Nth element if the return list corresponds to the status of Nth WorkSpec in the given list. Worker's
        status is one of WorkSpec.ST_finished, WorkSpec.ST_failed, WorkSpec.ST_cancelled, WorkSpec.ST_running,
        WorkSpec.ST_submitted. nativeExitCode and nativeStatus of WorkSpec can be arbitrary strings to help
        understanding behaviour of the resource and/or batch scheduler.

        :param workspec_list: a list of work specs instances
        :return: A tuple of return code (True for success, False otherwise) and a list of worker's statuses.
        :rtype: (bool, [string,])
        """
        retList = []
        for workSpec in workspec_list:
            # make logger
            tmpLog = self.make_logger(baseLogger, 'workerID={0}'.format(workSpec.workerID),
                                      method_name='check_workers')

            tmpLog.debug('check with {0}'.format(workSpec.batchID))

            r = self.check_batch(int(workSpec.batchID))

            newstatus = WorkSpec.ST_submitted
            
            if r[0].tag == 'error':
               err = r[0].find('error_msg').text
               newStatus = WorkSpec.ST_cancelled
               retList.append((newStatus, err))
               continue

            batch_status = r.find('state').text
            job_status = r.find('job').find('status').text
            tmpLog.debug('batch status: {0}'.format(batch_status))
            tmpLog.debug('batch status: {0}'.format(job_status))

            if job_status == 'queued':
                newStatus = WorkSpec.ST_submitted
            elif job_status == 'in_progress':
                newStatus = WorkSpec.ST_running
            elif job_status == 'error':
                newStatus = WorkSpec.ST_failed
            elif job_status == 'done':
                newStatus = WorkSpec.ST_finished

            tmpLog.debug('batchStatus {0} -> jobStatus {1}'.format(batch_status,
                                                                   newStatus))
 
            retList.append((newStatus, ''))

        return True, retList

def test(jobid):
    '''Test checking status'''
    from pandaharvester.harvestercore.work_spec import WorkSpec
    wspec = WorkSpec()
    wspec.batchID = jobid #

    monitor = BoincMonitor()
    print monitor.check_workers([wspec])

if __name__ == "__main__":
    import time, sys, urlparse
    if len(sys.argv) != 2:
        print "Please give ARC job id"
        sys.exit(1)
    #while True:
    test(sys.argv[1])
    #    time.sleep(2)

