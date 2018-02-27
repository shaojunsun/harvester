from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercloud import cernvm_aux

import googleapiclient.discovery
import os
import uuid

PROXY_PATH = harvester_config.pandacon.cert_file
USER_DATA_PATH = harvester_config.googlecloud.user_data_file

IMAGE = harvester_config.googlecloud.image
ZONE = harvester_config.googlecloud.zone
PROJECT = harvester_config.googlecloud.project
SERVICE_ACCOUNT_FILE = harvester_config.googlecloud.service_account_file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_FILE

compute = googleapiclient.discovery.build('compute', 'v1')


class GoogleVM():

    def __init__(self, work_spec):
        self.work_spec = work_spec
        self.name = 'google_vm_{0}_{1}'.format(harvester_config.master.harvester_id, work_spec.workerID)
        self.image = self.resolve_image_url()
        self.instance_type = self.resolve_instance_type()
        self.config = self.prepare_metadata()

    def resolve_image_url(self):
        """
        TODO: implement
        :param work_spec: worker specifications
        :return: URL pointing to the machine type to use
        """
        # Get the latest Debian Jessie image
        image_response = compute.images().getFromFamily(project=PROJECT, family='cernvm').execute()
        source_disk_image = image_response['selfLink']

        return source_disk_image

    def resolve_instance_type(self):
        """
        Resolves the ideal instance type for the work specifications. An overview on VM types can be found here: https://cloud.google.com/compute/docs/machine-types

        TODO: for the moment we will just assume we need the standard type, but in the future this function can be expanded
        TODO: to consider also custom VMs, hi/lo mem, many-to-one mode, etc.

        :param work_spec: worker specifications
        :return: instance type name
        """

        # Calculate the number of VCPUs
        cores = 8 # default value. TODO: probably should except if we don't find a suitable number
        standard_cores = [1, 2, 4, 8, 16, 32, 64, 96]
        for standard_core in standard_cores:
            if self.work_spec.nCore < standard_core:
                cores = standard_core
                break

        instance_type = 'zones/{0}/machineTypes/n1-standard-{1}'.format(ZONE, cores)

        return instance_type

    def configure_disk(self):
        """
        TODO: configure the ephemeral disk used by the VM
        :return: dictionary with disk configuration
        """

        return {}

    def prepare_metadata(self):
        """
        TODO: prepare any user data and metadata that we want to pass to the VM instance
        :return:
        """

        # read the proxy
        with open(PROXY_PATH, 'r') as proxy_file:
            proxy_string = proxy_file.read()

        with open(USER_DATA_PATH, 'r') as user_data_file:
            user_data = user_data_file.read()

        vm_name = 'harvester-{0}'.format(uuid.uuid4())

        {'name': vm_name,
         'machineType': 'zones/us-east1-b/machineTypes/n1-standard-1',

         # Specify the boot disk and the image to use as a source.
         'disks':
             [
                 {
                     'boot': True,
                     'autoDelete': True,
                     'initializeParams': {
                         'sourceImage': IMAGE,
                         'diskSizeGb': 50}
                 }
             ],

         # Specify a network interface with NAT to access the public internet
         'networkInterfaces':
             [
                 {
                     'network': 'global/networks/default',
                     'accessConfigs':
                         [
                             {
                                 'type': 'ONE_TO_ONE_NAT',
                                 'name': 'External NAT'
                             }
                         ]
                 }
             ],

         # Allow the instance to access cloud storage and logging.
         'serviceAccounts':
             [
                 {
                     'email': 'default',
                     'scopes':
                         [
                             'https://www.googleapis.com/auth/devstorage.read_write',
                             'https://www.googleapis.com/auth/logging.write'
                         ]
                 }
             ],

         'metadata':
             {
                 'items':
                     [
                         {
                             'key': 'user-data',
                             'value': cernvm_aux.encode_user_data(user_data)
                         },
                         {
                             'key': 'proxy',
                             'value': proxy_string
                         },
                         {
                             'key': 'panda_queue',
                             'value': 'CERN-PROD-preprod'
                         },
                     ]
             }
         }

        return config
