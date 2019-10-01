
import os
import random
import sys
import threading
import urllib
import socket
import zipfile

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.communicator_pool import CommunicatorPool
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper

from pandaharvester.boinc.submit_api import *
from pandaharvester.boinc.boinc_config import *

# setup base logger
baseLogger = core_utils.setup_logger()

class BoincSubmitter(PluginBase):
    '''Submitter for Boinc'''

    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        self.log = core_utils.make_logger(baseLogger)
        self.queueConfigMapper = QueueConfigMapper()
        self.communicatorPool = CommunicatorPool()
        self.nodeName = socket.gethostname()
        self.logBaseURL = "https://%s" % self.nodeName

    #get boinc output file url
    def get_output_url(self,batch_id):
        req = REQUEST()
        req.project = boinc_project['url']
        req.authenticator = boinc_project['authenticator']
        req.batch_id = batch_id
        r = get_output_files(req)
        return r

    def download_outputs(self,batch_id):
        url = self.get_output_url(batch_id)
        #save file
        dest = boinc_project['dest_log']
        filename = dest+str(batch_id)+"_result.zip"
        urllib.urlretrieve(url, filename)

        #unzip file
        if zipfile.is_zipfile(filename):
           with zipfile.ZipFile(filename, 'r') as zipObj:
                zipObj.extractall(dest)
           self.log.debug("file unpacked to:  %s" % dest)

        #output files
        batch_log = filename.replace('.zip', '.log')        
        stdout = filename.replace('.zip', '.out')
        stderr = filename.replace('.zip', '.err')

        return batch_log, stdout, stderr

    #get the number of jobs for the queue
    def get_num_jobs(self, queueName):
        if not queueName: 
           queueName = 'BOINC_BACKFILL'

        queueConfig = self.queueConfigMapper.get_queue(queueName)
        siteName = queueConfig.siteName
        jobs, errStr = self.communicatorPool.get_jobs(siteName, self.nodeName,
                                                  queueConfig.get_source_label(),
                                                  self.nodeName, queueConfig.maxWorkers,
                                                  queueConfig.getJobCriteria)

        if errStr =='OK': 
           njobs = len(jobs)
        else:
           njobs = 0
        self.log.debug("jobs: %s, err: %s" % (len(jobs),errStr))

        return njobs

    #make boinc file 
    def make_file(self,file_path):
        f = FILE_DESC()
        f.mode = 'inline'
        f.source = open(file_path, "r").read()
        return f 

    #make boinc batch 
    def make_batch(self):
        f_proxy = self.make_file(boinc_input_files['proxy_path'])
        f_wrapper = self.make_file(boinc_input_files['pilot_wrapper_path'])

        b = BATCH_DESC()
        b.project = boinc_project['url']
        b.authenticator = boinc_project['authenticator']
        b.app_name =  boinc_project['app_name']
        b.batch_name = boinc_project['batch_name']
        b.input_template_filename=boinc_project['input_template_filename']
        b.output_template_filename=boinc_project['output_template_filename']

        print(boinc_input_files['proxy_path'], boinc_input_files['pilot_wrapper_path'])
        print('+++++++++++++++++++++++++++++++++++')

        b.jobs = []
        job = JOB_DESC()
        job.files = [f_wrapper,f_proxy]

        job.rsc_fpops_est = 1*1e6
        job.command_line    = 'BOINC BOINC_BACKFILL'
        b.jobs.append(copy.copy(job))

        return b


    # submit workers
    def submit_workers(self, workspec_list):

        """Submit workers to a scheduling system like batch systems and computing elements.
        This method takes a list of WorkSpecs as input argument, and returns a list of tuples.
        Each tuple is composed of a return code and a dialog message.
        The return code could be True (for success), False (for permanent failures), or None (for temporary failures).
        If the return code is None, submission is retried maxSubmissionAttempts times at most which is defined
        for each queue in queue_config.json.
        Nth tuple in the returned list corresponds to submission status and dialog message for Nth worker
        in the given WorkSpec list.
        A unique identifier is set to WorkSpec.batchID when submission is successful,
        so that they can be identified in the scheduling system. It would be useful to set other attributes
        like queueName (batch queue name), computingElement (CE's host name), and nodeID (identifier of the node
        where the worker is running).
        :param workspec_list: a list of work specs instances
        :return: A list of tuples. Each tuple is composed of submission status (True for success,
        False for permanent failures, None for temporary failures) and dialog message
        :rtype: [(bool, string),]
        """
        self.log.debug("Handling workspec_list with %d items..." % len(workspec_list))
 
        retList = []
        for workSpec in workspec_list:

            self.log.debug("Worker(workerId=%s queueName=%s computingSite=%s nCore=%s status=%s " % (workSpec.workerID, 
                                                                                   workSpec.queueName,
                                                                                   workSpec.computingSite,
                                                                                   workSpec.nCore, 
                                                                                   workSpec.status) )
            #prepare and submit  boinc batch
            number_of_jobs = self.get_num_jobs(workSpec.computingSite)
                
            if number_of_jobs == 0 :
               continue
 
            b_batch = self.make_batch()

            res = submit_batch(b_batch)

            #check return message
            if res.tag == 'error': #failed
               self.log.debug('error: %s' % r.find('error_msg').text)
               retList.append((False, ''))
               continue

            workSpec.batchID = res.find('batch_id').text
            self.log.debug('batch submitted with id: %s' % res.find('batch_id').text)

            #boinc batch logs
            #batch_log, stdout, stderr  = self.download_outputs(int(workSpec.batchID))
            #workSpec.set_log_file('stdout', '{0}/{1}.log'.format(self.logBaseURL, batch_log))
            #workSpec.set_log_file('stderr', '{0}/{1}.err'.format(self.logBaseURL, stdout))
            #workSpec.set_log_file('batch_log', '{0}/{1}.log'.format(self.logBaseURL, stderr))

            retList.append((True, ''))

        return retList

def test():
    ##test submission##
    from pandaharvester.harvestercore.job_spec import JobSpec
    from pandaharvester.harvestercore.plugin_factory import PluginFactory
    from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper 
    import json

    queuename = 'BOINC_BACKFILL'
    queueconfmapper = QueueConfigMapper()
    queueconf = queueconfmapper.get_queue(queuename)
    print(queueconf)

    pluginfactory = PluginFactory()

    pandajob = '{"jobsetID": 11881, "logGUID": "88ee8a52-5c70-490c-a585-5eb6f48e4152", "cmtConfig": "x86_64-slc6-gcc49-opt", "prodDBlocks": "mc16_13TeV:mc16_13TeV.364168.Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV500_1000.merge.EVNT.e5340_e5984_tid11329621_00", "dispatchDBlockTokenForOut": "NULL,NULL", "destinationDBlockToken": "dst:CERN-PROD_DATADISK,dst:NDGF-T1_DATADISK", "destinationSE": "CERN-PROD_PRESERVATION", "realDatasets": "mc16_13TeV.364168.Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV500_1000.simul.HITS.e5340_e5984_s3126_tid11364822_00,mc16_13TeV.364168.Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV500_1000.simul.log.e5340_e5984_s3126_tid11364822_00", "prodUserID": "gingrich", "GUID": "A407D965-B139-A543-8851-A8E134A678D7", "realDatasetsIn": "mc16_13TeV:mc16_13TeV.364168.Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV500_1000.merge.EVNT.e5340_e5984_tid11329621_00", "nSent": 2, "cloud": "WORLD", "StatusCode": 0, "homepackage": "AtlasOffline/21.0.15", "inFiles": "EVNT.11329621._001079.pool.root.1", "processingType": "simul", "currentPriority": 900, "fsize": "129263662", "fileDestinationSE": "CERN-PROD_PRESERVATION,BOINC_MCORE", "scopeOut": "mc16_13TeV", "minRamCount": 1573, "jobDefinitionID": 0, "maxWalltime": 40638, "scopeLog": "mc16_13TeV", "transformation": "Sim_tf.py", "maxDiskCount": 485, "coreCount": 1, "prodDBlockToken": "NULL", "transferType": "NULL", "destinationDblock": "mc16_13TeV.364168.Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV500_1000.simul.HITS.e5340_e5984_s3126_tid11364822_00_sub0418634273,mc16_13TeV.364168.Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV500_1000.simul.log.e5340_e5984_s3126_tid11364822_00_sub0418634276", "dispatchDBlockToken": "NULL", "jobPars": "--inputEVNTFile=EVNT.11329621._001079.pool.root.1 --maxEvents=50 --postInclude \\"default:RecJobTransforms/UseFrontier.py\\" --preExec \\"EVNTtoHITS:simFlags.SimBarcodeOffset.set_Value_and_Lock(200000)\\" \\"EVNTtoHITS:simFlags.TRTRangeCut=30.0;simFlags.TightMuonStepping=True\\" --preInclude \\"EVNTtoHITS:SimulationJobOptions/preInclude.BeamPipeKill.py,SimulationJobOptions/preInclude.FrozenShowersFCalOnly.py\\" --skipEvents=4550 --firstEvent=5334551 --outputHITSFile=HITS.11364822._128373.pool.root.1 --physicsList=FTFP_BERT_ATL_VALIDATION --randomSeed=106692 --DBRelease=\\"all:current\\" --conditionsTag \\"default:OFLCOND-MC16-SDR-14\\" --geometryVersion=\\"default:ATLAS-R2-2016-01-00-01_VALIDATION\\" --runNumber=364168 --AMITag=s3126 --DataRunNumber=284500 --simulator=FullG4 --truthStrategy=MC15aPlus", "attemptNr": 2, "swRelease": "Atlas-21.0.15", "nucleus": "CERN-PROD", "maxCpuCount": 40638, "outFiles": "HITS.11364822._128373.pool.root.11,log.11364822._128373.job.log.tgz.1", "ddmEndPointOut": "CERN-PROD_DATADISK,NDGF-T1_DATADISK", "scopeIn": "mc16_13TeV", "PandaID": 3487584273, "sourceSite": "NULL", "dispatchDblock": "panda.11364822.07.05.GEN.0c9b1d3b-feec-411a-89e4-1cbf7347d70c_dis003487584270", "prodSourceLabel": "managed", "checksum": "ad:cd0bf10b", "jobName": "mc16_13TeV.364168.Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV500_1000.simul.e5340_e5984_s3126.3433643361", "ddmEndPointIn": "NDGF-T1_DATADISK", "taskID": 11364822, "logFile": "log.11364822._128373.job.log.tgz.1"}'

    pandajob = json.loads(pandajob)
    jspec = JobSpec()
    jspec.convert_job_json(pandajob)
    jspec.computingSite = queuename
    jspeclist = [jspec]

    maker = pluginfactory.get_plugin(queueconf.workerMaker)
    #wspec = maker.make_worker(jspeclist, queueconf, 'boinc')
    wspec = maker.make_worker(jspeclist, queueconf, 'BOINC_BACKFILL')
    wspec.hasJob = 1
    wspec.set_jobspec_list(jspeclist)

    bs = BoincSubmitter()
    print bs.submit_workers([wspec])

if __name__ == '__main__':

   test()
