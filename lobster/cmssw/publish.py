import os
import subprocess
import time
import sys
import uuid
from zlib import adler32
import gzip
import yaml
import shutil

from RestClient.ErrorHandling.RestClientExceptions import HTTPError
from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.FwkJobRep.SiteLocalConfig import SiteLocalConfig
from ProdCommon.FwkJobRep.TrivialFileCatalog import readTFC
from ProdCommon.MCPayloads.WorkflowTools import createPSetHash
sys.path.insert(0, '/cvmfs/cms.cern.ch/crab/CRAB_2_10_2_patch2/external/dbs3client')
from dbs.apis.dbsClient import DbsApi

import jobit
from lobster import util
from lobster.job import apply_matching

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

def migrate_parents(parents, dbs_local, dbs_global, dbs_migrator):
   parent_blocks_to_migrate = []
   for parent in parents:
       print "Looking in the local DBS for blocks associated with the parent lfn:\n%s..." % parent
       parent_blocks = dbs_local.listBlocks(logical_file_name=parent)
       if parent_blocks:
           print 'Parent blocks found: no migration required.'
       else:
           print 'No parent blocks found in the local DBS, searching the global DBS...'
           dbs_output = dbs_global.listBlocks(logical_file_name=parent)
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
               migration_status = dbs_migrator.statusMigration(block_name=block)
               if not migration_status:
                   print 'Block will be migrated: %s' % block
                   dbs_output = dbs_migrator.submitMigration({'migration_url': self.dbs_url_global, 'migration_input': block})
                   all_migrated = False
               else:
                   migrated.append(block)
                   all_migrated = all_migrated and check_migration(migration_status[0]['migration_status'])

           for block in migrated:
               migration_status = dbs_migrator.statusMigration(block_name=block)
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
    def __init__(self, username, dataset, dbs_global, publish_hash, publish_label, release, pset_hash, gtag):
        self.username = username
        self.dataset = dataset
        self.publish_label = publish_label
        self.publish_hash = publish_hash
        self.release = release
        self.pset_hash = pset_hash
        self.gtag = gtag

        storage_path = '/cvmfs/cms.cern.ch/SITECONF/%s/PhEDEx/storage.xml' % os.environ['CMS_LOCAL_SITE']
        self.catalog = readTFC(storage_path)

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

        self.set_primary_dataset(dataset, dbs_global)
        self.set_dataset(1)
        self.set_block(1)

    def set_primary_dataset(self, prim_ds, dbs_reader):
        output = dbs_reader.listPrimaryDatasets(primary_ds_name=prim_ds)
        if len(output) > 0:
            self.data['primds'] = dict((k, v) for k, v in output[0].items() if k!= 'primary_ds_id')
        else:
            print "Cannot find any information about the primary dataset %s in the global dbs, using default parameters..." % prim_ds
            self.data['primds']['create_by'] = ''
            self.data['primds']['primary_ds_type'] = 'NOTSET'
            self.data['primds']['primary_ds_name'] = prim_ds
            self.data['primds']['creation_date'] = ''

    def set_dataset(self, version):
        #TODO: VERSION INCREMENTING
        processed_ds_name = '%s-%s-v%d' % (self.username, self.publish_label+'_'+self.publish_hash, version)

        self.data['dataset']['primary_ds_name'] = self.dataset
        self.data['dataset']['create_by'] = self.username
        self.data['dataset']['dataset_access_type'] = 'VALID'
        self.data['dataset']['data_tier_name'] = 'USER'
        self.data['dataset']['last_modified_by'] = self.username
        self.data['dataset']['creation_date'] = int(time.time())
        self.data['dataset']['processed_ds_name'] = processed_ds_name
        self.data['dataset']['last_modification_date'] = int(time.time())
        self.data['dataset']['dataset'] = u'/%s/%s/USER' % (self.dataset, processed_ds_name)
        self.data['dataset']['processing_version'] = version
        self.data['dataset']['acquisition_era_name'] = self.username
        self.data['dataset']['physics_group_name'] = 'NoGroup'

    def set_block(self, version):
        site_config_path = '/cvmfs/cms.cern.ch/SITECONF/%s/JobConfig/site-local-config.xml' % os.environ['CMS_LOCAL_SITE']

        self.data['block']['create_by'] = self.username
        self.data['block']['creation_date'] = int(time.time())
        self.data['block']['open_for_writing'] = 1
        self.data['block']['origin_site_name'] = SiteLocalConfig(site_config_path).localStageOutSEName()
        self.data['block']['block_name'] = self.data['dataset']['dataset']+'#'+str(uuid.uuid4())
        self.data['block']['file_count'] = 0
        self.data['block']['block_size'] = 0

    def reset(self):
        self.data['files'] = []
        self.data['file_conf_list'] = []
        self.data['file_parent_list'] = []
        self.data['dataset_conf_list'] = []

        bname = self.data['block']['block_name']
        self.data['block']['block_name'] = bname[:bname.rfind('#')+1]+str(uuid.uuid4())
        self.data['block']['file_count'] = 0
        self.data['block']['block_size'] = 0

    def add_dataset_config(self, app_name='cmsRun', output_label='Merged'):
       dataset_config = {'release_version': self.release,
                         'pset_hash': self.pset_hash,
                         'app_name': app_name, #TODO PROPERLY
                         'output_module_label': output_label, #TODO PROPERLY
                         'global_tag': self.gtag}

       self.data['dataset_conf_list'].append(dataset_config)

    def add_file_config(self, LFN, app_name='cmsRun', output_label='Merged'):
       conf_dict = {'release_version': self.release,
                    'pset_hash': self.pset_hash,
                    'lfn': LFN,
                    'app_name': app_name, #TODO PROPERLY
                    'output_module_label': output_label, #TODO PROPERLY
                    'global_tag': self.gtag}

       self.data['file_conf_list'].append(conf_dict)

    def add_file_parents(self, LFN, report):
        for f in report.inputFiles:
            parent = {'logical_file_name': LFN, 'parent_logical_file_name': f['LFN']}
            if parent not in self.data['file_parent_list']:
                self.data['file_parent_list'].append(parent)

    def add_file(self, LFN, output):
        lumi_dict_to_list = lambda d: [{'run_num': run, 'lumi_section_num': lumi} for run in d.keys() for lumi in d[run]]
        PFN = self.catalog.matchLFN('direct', LFN)
        c = subprocess.Popen('cksum %s' % PFN, shell=True, stdout=subprocess.PIPE)
        cksum, size = c.stdout.read().split()[:2]

        file_dict = {'check_sum': int(cksum),
                     'file_lumi_list': lumi_dict_to_list(output['Runs']),
                     'adler32': get_adler32(PFN),
                     'event_count': int(output['TotalEvents']),
                     'file_type': output['FileType'],
                     'last_modified_by': self.username,
                     'logical_file_name': LFN,
                     'file_size': int(size),
                     'last_modification_date': int(os.path.getmtime(PFN))}
#                     'md5': 'NOTSET', #TODO EVENTUALLY
#                     'auto_cross_section':  0.0 #TODO EVENTUALLY

        self.data['files'].append(file_dict)

        self.data['block']['block_size'] += int(size)
        self.data['block']['file_count'] += 1

    def get_LFN(self, PFN):
        #see https://twiki.cern.ch/twiki/bin/viewauth/CMS/DMWMPG_Namespace#store_user_and_store_temp_user
        LFN = os.path.join('/store/user',
                           self.username,
                           self.dataset,
                           self.publish_label+'_'+self.publish_hash,
                           os.path.basename(PFN))

        matched = self.catalog.matchLFN('direct', LFN)
        matched_dir = os.path.dirname(matched)
        if PFN != matched and not os.path.isfile(matched):
            if not os.path.isdir(matched_dir):
                os.makedirs(matched_dir)
            shutil.move(PFN, matched_dir)

        return LFN

    def __getitem__(self, item):
        return self.data[item]

    def __setitem__(self, key, value):
        self.data[key] = value

def publish(args):
    with open(args.configfile) as f:
        config = yaml.load(f)

    config = apply_matching(config)

    if len(args.labels) == 0:
        args.labels = [task['label'] for task in config.get('tasks', [])]

    wdir = config['workdir']
    db = jobit.JobitStore(config)
    user = config.get('publish user', os.environ['USER'])

    dbs_url = 'https://cmsweb.cern.ch/dbs/prod/{0}/'.format(config.get('dbs instance', 'phys03'))
    dbs_url_global = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
    dbs_global = DbsApi(url=dbs_url_global)
    dbs_local = DbsApi(os.path.join(dbs_url, 'DBSWriter'))
    dbs_local_reader = DbsApi(url=dbs_url+'DBSReader')
    dbs_migrator = DbsApi(url=os.path.join(dbs_url, 'DBSMigrate'))

    for label in args.labels:
        (dset,
         stageout_path,
         release,
         gtag,
         publish_label,
         cfg,
         pset_hash,
         ds_id,
         publish_hash) = [str(x) for x in db.dataset_info(label)]

        dset = dset.strip('/').split('/')[0]
        if not pset_hash or pset_hash == 'None':
            print 'The parameter set hash has not yet been calculated, doing it now... (this may take a few minutes)'
            cfg_path = os.path.join(wdir, label, os.path.basename(cfg))
            tmp_path = cfg_path.replace('.py', '_tmp.py')
            with open(cfg_path, 'r') as infile:
                with open(tmp_path, 'w') as outfile:
                    fix = "import sys \nif not hasattr(sys, 'argv'): sys.argv = ['{0}']\n"
                    outfile.write(fix.format(tmp_path))
                    outfile.write(infile.read())
            try:
                pset_hash = createPSetHash(tmp_path)[-32:]
                db.update_pset_hash(pset_hash, label)
            except:
                print 'Error calculating cmssw parameter set hash!  Continuing with empty hash...'
            os.remove(tmp_path)

        block = BlockDump(user, dset, dbs_global, publish_hash, publish_label, release, pset_hash, gtag)

        if len(dbs_local.listAcquisitionEras(acquisition_era_name=user)) == 0:
            try:
                dbs_local.insertAcquisitionEra({'acquisition_era_name': user})
            except Exception, ex:
                print ex
        try:
            dbs_local.insertPrimaryDataset(block.data['primds'])
            dbs_local.insertDataset(block.data['dataset'])
        except Exception, ex:
            print ex
            raise

        jobs = db.finished_jobs(label)

        first_job = 0
        print 'Found %d successful %s jobs to publish.' % (len(jobs), label)
        while first_job < len(jobs):
            block.reset()
            blocks = []
            chunk = jobs[first_job:first_job+args.block_size]
            print 'Preparing DBS entry for block of %i jobs.' % len(chunk)

            for job, merged_job in chunk:
                status = 'successful' if merged_job == 0 else 'merged'
                id = job if merged_job == 0 else merged_job
                tag = str(job) if merged_job == 0 else 'merged_{0}'.format(merged_job)

                f = gzip.open(os.path.join(wdir, label, status, util.id2dir(id), 'report.xml.gz'), 'r')
                report = readJobReport(f)[0]
                PFN = os.path.join(stageout_path, report.files[0]['PFN'].replace('.root', '_%s.root' % tag))
                LFN = block.get_LFN(PFN)

                print 'Adding %s to block...' % LFN
                block.add_file_parents(LFN, report)
                block.add_file_config(LFN)
                block.add_file(LFN, report.files[0])
                block.add_dataset_config()
                blocks += [(block['block']['block_name'], job, merged_job)]

            if args.migrate_parents:
                parents_to_migrate = list(set([p['parent_logical_file_name'] for p in block['file_parent_list']]))
                migrate_parents(parents_to_migrate, dbs_local, dbs_global, dbs_migrator)

            if len(block.data['files']) > 1:
                try:
                    dbs_local.insertBulkBlock(block.data)
                    db.update_published(blocks)
                    lfn_string = '\n'.join([d['logical_file_name'] for d in block['files']])
                    info = (linebreak, block['block']['block_name'], lfn_string, linebreak)
                    print '%s\nBlock inserted:\n%s\n\nFiles in block:\n%s%s' % info
                except HTTPError, e:
                    print e

            first_job += args.block_size
