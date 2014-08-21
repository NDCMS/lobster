import os
import subprocess
import time
import sys
import uuid
from zlib import adler32
import gzip
import yaml

from RestClient.ErrorHandling.RestClientExceptions import HTTPError
from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.FwkJobRep.SiteLocalConfig import SiteLocalConfig
from ProdCommon.MCPayloads.WorkflowTools import createPSetHash
sys.path.insert(0, '/cvmfs/cms.cern.ch/crab/CRAB_2_10_2_patch2/external/dbs3client')
from dbs.apis.dbsClient import DbsApi

import jobit
import merge

linebreak = '\n'+''.join(['*']*80)

def get_adler32(filename):
    sum = 1L
    with open(filename, 'rb') as file:
        sum = adler32(file.read(1000000000), sum)

    return '%x' % (sum & 0xffffffffL)

def check_migration(status):
    successful = False

    if status == 0:
        print 'Migration required.'
    elif status == 1:
        print 'Migration in process.'
        time.sleep(15)
    elif status == 2:
        print 'Migration successful.'
        successful = True
    elif status == 3:
        print 'Migration failed.'

    return successful

def required_path(path, username, primary_ds, publish_label, publish_hash, version):
    #TODO: handle storage mapping in a general way
    #see https://twiki.cern.ch/twiki/bin/viewauth/CMS/DMWMPG_Namespace#store_user_and_store_temp_user
    #required_path = os.path.join('/store', 'user', username, primary_ds, publish_label+'_'+publish_hash, 'v%d' % version)
    required_path = os.path.join('/store', 'user', username, primary_ds, publish_label+'_'+publish_hash)
    if path != required_path:
        if not os.path.exists(required_path):
            if not os.path.isdir(os.path.dirname(required_path)):
                os.makedirs(os.path.dirname(required_path))
            os.symlink(path, required_path)

    return required_path

class Publisher():
   def __init__(self, config, dir, label):
       self.label = label
       self.dir = dir
       self.db = jobit.JobitStore(config)
       self.config = config
       self.user = config.get('publish user', os.environ['USER'])
# Not sure if 'create_by' should be username or grid info, it is mixed in DBS: for now, use username
#        handle = subprocess.Popen(['grid-proxy-info', '-identity'], stdout=subprocess.PIPE)
#        self.user_info = handle.stdout.read().strip()
#        handle.terminate()
       (dset,
        self.path,
        self.release,
        self.gtag,
        self.publish_label,
        cfg,
        self.pset_hash,
        self.ds_id,
        self.publish_hash) = [str(x) for x in self.db.dataset_info(label)]

       self.dbs_url = config.get('dbs url', 'https://cmsweb-testbed.cern.ch/dbs/int/phys03/')
       self.dbs_url_global = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
       self.dbs_global = DbsApi(url=self.dbs_url_global)
       self.dbs_local = DbsApi(os.path.join(self.dbs_url, 'DBSWriter'))
       self.dbs_local_reader = DbsApi(url=self.dbs_url+'DBSReader')
       self.dbs_migrator = DbsApi(url=os.path.join(self.dbs_url, 'DBSMigrate'))

       if not self.pset_hash or self.pset_hash == 'None':
           print 'The parameter set hash has not yet been calculated, doing it now... (this may take a few minutes)'
           try:
               self.pset_hash = createPSetHash(os.path.join(dir, label, os.path.basename(cfg)))[-32:]
               self.db.update_datasets('pset_hash', self.pset_hash, label)
           except:
               print 'Error calculating cmssw parameter set hash!  Continuing with empty hash for now...' #FIXME

       self.dset = dset.strip('/').split('/')[0]

   def publish(self, max_jobs):
       self.required_path = required_path(self.path, self.user, self.dset, self.publish_label, self.publish_hash, 1)

       self.block_dump = BlockDump(self.user)
       self.block_dump.set_primary_dataset(self.dset, self.dbs_global)
       self.block_dump.set_dataset(self.publish_label, self.dset, 1, self.publish_hash)
       self.block_dump.set_block(self.publish_label, self.dset, 1, self.publish_hash)
       if len(self.dbs_local.listAcquisitionEras(acquisition_era_name=self.user)) == 0:
           try:
               self.dbs_local.insertAcquisitionEra({'acquisition_era_name': self.user})
           except Exception, ex:
               print ex
       try:
           self.dbs_local.insertPrimaryDataset(self.block_dump.data['primds'])
           self.dbs_local.insertDataset(self.block_dump.data['dataset'])
       except Exception, ex:
           print ex
           raise

       jobs = [x[0] for x in self.db.finished_jobs(self.ds_id)]

       first_job = 0
       print 'Found %d successful %s jobs to publish.' % (len(jobs), self.label)
       while first_job < len(jobs):
           self.block_dump.reset_block()

           jstring = ', '.join([str(x) for x in jobs[first_job:first_job+max_jobs]])
           print 'Preparing DBS entry for block of %i jobs: %s' % (len(jobs[first_job:first_job+max_jobs]), jstring)
           successful_jobs = jobs[first_job:first_job+max_jobs]
           for job in jobs[first_job:first_job+max_jobs]:
               f = gzip.open(os.path.join(self.dir, self.label, 'successful', str(job), 'report.xml.gz'), 'r')
               for report in readJobReport(f)[0].files:
                   filename = report['PFN'].replace('.root', '_%s.root' % job) #TODO: handle this properly
                   LFN = os.path.join(self.required_path, filename)
                   try:
                       self.block_dump.add_file_parent(LFN, report)
                       self.block_dump.add_file_config(LFN, self.release, self.pset_hash, self.gtag)
                       self.block_dump.add_file(LFN, report)
                       self.block_dump.add_dataset_config(self.release, self.pset_hash, self.gtag)
                       print 'Adding %s to block...' % LFN

                   except:
                       successful_jobs.remove(job)
                       continue

#           self.block_dump.data = dict((k, v) for k, v in self.block_dump.data.items() if k not in ['processing_era'])
           if args.migrate_parents:
               parents_to_migrate = list(set([p['parent_logical_file_name'] for p in self.block_dump['file_parent_list']]))
               self.migrate_parents(parents_to_migrate)

           failed_jobs = set(successful_jobs) - set(jobs[first_job:first_job+max_jobs])
           for job in failed_jobs:
               self.db.update_jobits(job, failed=True, return_code=500)

           if len(successful_jobs) > 0:
               try:
                   self.dbs_local.insertBulkBlock(self.block_dump.data)
                   blocks = [(self.block_dump['block']['block_name'], job) for job in successful_jobs]
                   lfn_string = '\n'.join([d['logical_file_name'] for d in self.block_dump['files']])
                   self.db.update_published(blocks)
                   print '%s\nBlock inserted:\n%s\n\nFiles in block:\n%s%s' % (linebreak, self.block_dump['block']['block_name'], lfn_string, linebreak)
               except HTTPError, e:
                   print e

           first_job += max_jobs

   def migrate_parents(self, parents):
       parent_blocks_to_migrate = []
       for parent in parents:
           print "Looking in the local DBS for blocks associated with the parent lfn:\n%s..." % parent
           parent_blocks = self.dbs_local_reader.listBlocks(logical_file_name=parent)
           if parent_blocks:
               print 'Parent blocks found: no migration required.'
           else:
               print 'No parent blocks found in the local DBS, searching the global DBS...'
               dbs_output = self.dbs_global.listBlocks(logical_file_name=parent)
               if dbs_output:
                   parent_blocks_to_migrate.extend([entry['block_name'] for entry in dbs_output])
               else:
                   print 'No parent block found the global or local DBS: exiting...'
                   exit()

       parent_blocks_to_migrate = list(set(parent_blocks_to_migrate))
       if len(parent_blocks_to_migrate) > 0:
           print 'The following files will be migrated: %s...' % ', '.join(parent_blocks_to_migrate)

           migration_complete = False
           retries = 0
           while (retries < 5 and not migration_complete):
               migrated = []
               all_migrated = True
               for block in parent_blocks_to_migrate:
                   migration_status = self.dbs_migrator.statusMigration(block_name=block)
                   if not migration_status:
                       print 'Block will be migrated: %s' % block
                       dbs_output = self.dbs_migrator.submitMigration({'migration_url': self.dbs_url_global, 'migration_input': block})
                       all_migrated = False
                   else:
                       migrated.append(block)
                       all_migrated = all_migrated and check_migration(migration_status[0]['migration_status'])

               for block in migrated:
                   migration_status = self.dbs_migrator.statusMigration(block_name=block)
                   all_migrated = migration_status and all_migrated and check_migration(migration_status[0]['migration_status'])

               migration_complete = all_migrated
               print 'Migration not complete, waiting 15 seconds and checking again...'
               time.sleep(15)
               retries += 1

           if not migration_complete:
               print 'Migration from global to local dbs failed: exiting...'
               exit()
           else:
               print 'Migration of all files complete.'

class BlockDump:
    def __init__(self, username):
        self.username = username
        self.data = {'dataset_conf_list': [],
                     'file_conf_list': [],
                     'files': [],
                     'block': {},
                     'processing_era': {},
                     'acquisition_era': {},
                     'primds': {},
                     'dataset': {},
                     'file_parent_list': []}

        self.data['acquisition_era']['acquisition_era_name'] = self.username #see https://twiki.cern.ch/twiki/bin/viewauth/CMS/DMWMPG_PrimaryDatasets#User_Data
        self.data['acquisition_era']['start_date'] = int(time.strftime('%Y'))

        self.data['processing_era']['create_by'] = 'lobster'
        self.data['processing_era']['processing_version'] = 1
        self.data['processing_era']['description'] = 'lobster'

    def set_dataset(self, publish_label, primary_ds_name, version, publish_hash):
        processed_ds_name = '%s-%s-v%d' % (self.username, publish_label+'_'+publish_hash, version) #TODO: VERSION INCREMENTING

        self.data['dataset']['primary_ds_name'] = primary_ds_name
        self.data['dataset']['create_by'] = self.username
        self.data['dataset']['dataset_access_type'] = 'VALID'
        self.data['dataset']['data_tier_name'] = 'USER'
        self.data['dataset']['last_modified_by'] = self.username
        self.data['dataset']['creation_date'] = int(time.time())
        self.data['dataset']['processed_ds_name'] = processed_ds_name
        self.data['dataset']['last_modification_date'] = int(time.time())
        self.data['dataset']['dataset'] = u'/%s/%s/USER' % (primary_ds_name, processed_ds_name)
        self.data['dataset']['processing_version'] = version
        self.data['dataset']['acquisition_era_name'] = self.username
        self.data['dataset']['physics_group_name'] = 'NoGroup'

    def set_block(self, publish_label, primary_ds_name, version, publish_hash):
        if self.data['dataset'] == {}:
            self.set_dataset(publish_label, processed_ds_name, version, publish_hash)

        site_config_path = '/cvmfs/cms.cern.ch/SITECONF/%s/JobConfig/site-local-config.xml' % os.environ['CMS_LOCAL_SITE']

        self.data['block']['create_by'] = self.username
        self.data['block']['creation_date'] = int(time.time())
        self.data['block']['open_for_writing'] = 1
        self.data['block']['origin_site_name'] = SiteLocalConfig(site_config_path).localStageOutSEName()
        self.data['block']['block_name'] = self.data['dataset']['dataset']+'#'+str(uuid.uuid4())
        self.data['block']['file_count'] = 0
        self.data['block']['block_size'] = 0

    def reset_block(self):
        self.data['files'] = []
        self.data['file_conf_list'] = []
        self.data['file_parent_list'] = []
        self.data['dataset_conf_list'] = []

        bname = self.data['block']['block_name']
        self.data['block']['block_name'] = bname[:bname.rfind('#')+1]+str(uuid.uuid4())
        self.data['block']['file_count'] = 0
        self.data['block']['block_size'] = 0

    def add_dataset_config(self, release, pset_hash, gtag, app_name='cmsRun', output_label='Merged'):

       dataset_config = {'release_version': release,
                         'pset_hash': pset_hash,
                         'app_name': app_name, #TODO PROPERLY
                         'output_module_label': output_label, #TODO PROPERLY
                         'global_tag': gtag}

       self.data['dataset_conf_list'].append(dataset_config)

    def set_primary_dataset(self, prim_ds, dbs_reader):
        output = dbs_reader.listPrimaryDatasets(primary_ds_name=prim_ds)
        if len(output) > 0:
            self.data['primds'] = dict((k, v) for k, v in output[0].items() if k!= 'primary_ds_id') #for some reason dbs doesn't like its own dict keys
        else:
            print "Cannot find any information about the primary dataset %s in the global dbs, using default parameters..." % prim_ds
            self.data['primds']['create_by'] = ''
            self.data['primds']['primary_ds_type'] = 'NOTSET'
            self.data['primds']['primary_ds_name'] = prim_ds
            self.data['primds']['creation_date'] = ''

    def add_file_config(self, LFN, release, pset_hash, gtag, app_name='cmsRun', output_label='Merged'):
       conf_dict = {'release_version': release,
                    'pset_hash': pset_hash,
                    'lfn': LFN,
                    'app_name': app_name, #TODO PROPERLY
                    'output_module_label': output_label, #TODO PROPERLY
                    'global_tag': gtag}

       self.data['file_conf_list'].append(conf_dict)

    def add_file_parent(self, LFN, report):
       p_list = [{'logical_file_name': LFN, 'parent_logical_file_name': PLFN} for PLFN in report.parentLFNs()]
       self.data['file_parent_list'].extend(p_list)

    def add_file(self, LFN, report):
        lumi_dict_to_list = lambda d: [{'run_num': run, 'lumi_section_num': lumi} for run in d.keys() for lumi in d[run]]

        c = subprocess.Popen('cksum %s' % LFN, shell=True, stdout=subprocess.PIPE)
        cksum, size = c.stdout.read().split()[:2]

        file_dict = {'check_sum': int(cksum),
                     'file_lumi_list': lumi_dict_to_list(report['Runs']),
                     'adler32': get_adler32(LFN),
                     'event_count': int(report['TotalEvents']),
                     'file_type': report['FileType'],
                     'last_modified_by': self.username,
                     'logical_file_name': LFN,
                     'file_size': int(size),
                     'last_modification_date': int(os.path.getmtime(LFN))}
#                     'md5': 'NOTSET', #TODO EVENTUALLY
#                     'auto_cross_section':  0.0 #TODO EVENTUALLY

        self.data['files'].append(file_dict)

        self.data['block']['block_size'] += int(size)
        self.data['block']['file_count'] += 1

    def __getitem__(self, item):
        return self.data[item]

    def __setitem__(self, key, value):
        self.data[key] = value

def publish(args):
    with open(args.configfile) as f:
        config = yaml.load(f)

    dir = config['workdir']

    if len(args.labels) == 0:
        args.labels = [task['label'] for task in config.get('tasks', [])]

    for label in args.labels:
        publisher = Publisher(config, dir, label)

        if args.clean:
            publisher.clean()
        else:
            publisher.publish(args.block_size)
