# TODO
# * make sure that primary dataset name is allowed - fail with useful message
# * use username in PFN construction
# * no all CAPS variable names - should be reserved for constants
# * fix filetype setting
# * migrate parents
import daemon
import json
import logging
import os
import shutil
import subprocess
import sys
import time
import uuid

from RestClient.ErrorHandling.RestClientExceptions import HTTPError
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.Storage.SiteLocalConfig import SiteLocalConfig
from WMCore.Storage.TrivialFileCatalog import readTFC
from dbs.apis.dbsClient import DbsApi

from lobster import fs, se, util
from lobster.core.command import Command
from lobster.core.unit import UnitStore

logger = logging.getLogger('lobster.publish')


def hash_pset(cfg, label, args):
    try:
        logger.info('calculating parameter set hash now (may take a few minutes)')
        cfg_path = os.path.join(args.config.workdir, label, os.path.basename(cfg))
        tmp_path = cfg_path.replace('.py', '_tmp.py')
        with open(cfg_path, 'r') as infile:
            with open(tmp_path, 'w') as outfile:
                fix = "import sys \nif not hasattr(sys, 'argv'): sys.argv = ['{0}']\n"
                outfile.write(fix.format(tmp_path))
                outfile.write(infile.read())
        return subprocess.check_output(['edmConfigHash', tmp_path]).splitlines()[-1]
    finally:
        os.remove(tmp_path)


def check_migration(status):
    successful = False

    if status == 0:
        logger.info('migration required')
    elif status == 1:
        logger.info('migration in process')
        time.sleep(15)
    elif status == 2:
        logger.info('migration successful')
        successful = True
    elif status == 3:
        logger.info('migration failed')

    return successful


def migrate_parents(parents, dbs):
    parent_blocks_to_migrate = []
    for parent in parents:
        logger.info(
            "looking in the local DBS for blocks associated with the parent lfn %s" % parent)
        parent_blocks = dbs['local'].listBlocks(logical_file_name=parent)
        if parent_blocks:
            logger.info('parent blocks found: no migration required')
        else:
            logger.info(
                'no parents blocks found in the local DBS, searching global DBS')
            dbs_output = dbs['global'].listBlocks(logical_file_name=parent)
            if dbs_output:
                parent_blocks_to_migrate.extend(
                    [entry['block_name'] for entry in dbs_output])
            else:
                logger.critical('unable to find parent blocks in the local or global DBS')
                logger.critical("verify parent blocks exist in DBS, or set 'migrate parents: False' in your lobster configuration")
                exit()

    parent_blocks_to_migrate = list(set(parent_blocks_to_migrate))
    if len(parent_blocks_to_migrate) > 0:
        logger.info('the following files will be migrated: {}'.format(', '.join(parent_blocks_to_migrate)))

        migration_complete = False
        retries = 0
        while (retries < 5 and not migration_complete):
            migrated = []
            all_migrated = True
            for block in parent_blocks_to_migrate:
                migration_status = dbs[
                    'migrator'].statusMigration(block_name=block)
                if not migration_status:
                    logger.info('block will be migrated: %s' % block)
                    dbs_output = dbs['migrator'].submitMigration(
                        {'migration_url': 'https://cmsweb.cern.ch/dbs/prod/global/', 'migration_input': block})
                    all_migrated = False
                else:
                    migrated.append(block)
                    all_migrated = all_migrated and check_migration(
                        migration_status[0]['migration_status'])

            for block in migrated:
                migration_status = dbs[
                    'migrator'].statusMigration(block_name=block)
                all_migrated = migration_status and all_migrated and check_migration(
                    migration_status[0]['migration_status'])

            migration_complete = all_migrated
            logger.info(
                'migration not complete, waiting 15 seconds before checking again')
            time.sleep(15)
            retries += 1

        if not migration_complete:
            logger.critical('migration from global to local dbs failed')
            exit()
        else:
            logger.info('migration of all files complete')


class Publish(Command):

    def __init__(self):
        # FIXME this should really be SE specific
        storage_path = '/cvmfs/cms.cern.ch/SITECONF/{}/PhEDEx/storage.xml'.format(os.environ['CMS_LOCAL_SITE'])
        self.__catalog = readTFC(storage_path)
        self.__dbs = {}

    @property
    def help(self):
        return 'publish results in the CMS Data Aggregation System'

    def setup(self, argparser):
        general = argparser.add_argument_group('general options')
        general.add_argument('-f', '--foreground', action='store_true', default=False,
                             help='do not daemonize;  run in the foreground instead')
        general.add_argument('--workflows', nargs='*', metavar='WORKFLOW',
                             help='workflows to publish (default: all workflows)')
        general.add_argument('--datasets', nargs='*', metavar='DATASET',
                             help='dataset names to use for publication')

        details = argparser.add_argument_group('publishing options')
        details.add_argument('--block-size', dest='block_size', type=int, default=50, metavar='SIZE',
                             help='number of files to publish per file block (default: 50)')
        details.add_argument('--instance', default='phys03', help='DBS instance to publish to (default: phys03)')
        details.add_argument('--migrate-parents', dest='migrate_parents', default=False, action='store_true',
                             help='migrate parents to local DBS')
        details.add_argument('--user', default=None,
                             help='username to use for publication (default: from SiteDB)')
        details.add_argument('--version', default=1, type=int, help='version of the dataset (default: 1)')

    def __get_config(self, args, label, pset_hash):
        workflow = getattr(args.config.workflows, label)
        if workflow.pset and workflow.globaltag is None:
            with util.PartiallyMutable.unlock():
                workflow.determine_globaltag([args.config.workdir, args.config.startup_directory])
        return {
            'release_version': workflow.version,
            'pset_hash': pset_hash,
            'app_name': workflow.command,
            'output_module_label': 'o',
            'global_tag': workflow.globaltag
        }

    def __get_distinguished_name(self):
        p = subprocess.Popen(["voms-proxy-info", "-identity"],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        id_, err = p.communicate()
        return id_.strip()

    def __get_user(self):
        db = SiteDBJSON({'cacheduration': 24, 'logger': logging.getLogger("WMCore")})
        return db.dnUserName(dn=self.__get_distinguished_name())

    def match_pfn(self, relative, lfn):
        matched = self.__catalog.matchLFN('direct', lfn)
        matched_dir = os.path.dirname(matched)
        pfn = fs.lfn2pfn(relative, instance=se.Local)
        if os.path.isfile(pfn):
            if not os.path.isfile(matched):
                if not os.path.isdir(matched_dir):
                    os.makedirs(matched_dir)
                shutil.move(pfn, matched_dir)
        else:
            if not os.path.isfile(matched):
                return None, None
        return pfn, matched

    def insert_dataset(self, dbs, primary, user, label, hash_, version):
        primary_dataset = {
            # 'create_by': '',
            'primary_ds_type': 'NOTSET',
            'primary_ds_name': primary,
            'creation_date': ''
        }
        output = dbs['global'].listPrimaryDatasets(primary_ds_name=primary)
        if len(output) > 0:
            primary_dataset = dict((k, v) for k, v in output[0].items() if k != 'primary_ds_id')

        dataset = {
            'primary_ds_name': primary,
            'physics_group_name': 'NoGroup',
            'processed_ds_name': '{}-{}-v{}'.format(user, label + '_' + hash_, version),
            'dataset_access_type': 'VALID',
            'data_tier_name': 'USER',  # FIXME set correctly
            'acquisition_era_name': user,
            'processing_version': version
        }
        dataset.update({
            'dataset': '/{}/{}/{}'.format(
                dataset['primary_ds_name'],
                dataset['processed_ds_name'],
                dataset['data_tier_name']
            )
        })

        try:
            dbs['local'].insertPrimaryDataset(primary_dataset)
            dbs['local'].insertDataset(dataset)
        except Exception as ex:
            logger.exception(ex)

        return primary_dataset, dataset

    # def insert_dataset(self, dbs, primary, user, label, hash_):
    def prepare_block(self, dataset, user):
        site_config_path = '/cvmfs/cms.cern.ch/SITECONF/{}/JobConfig/site-local-config.xml'.format(os.environ['CMS_LOCAL_SITE'])
        site = SiteLocalConfig(site_config_path).siteName

        block = {
            # 'site_list': [site],
            'block_name': '{}#{}'.format(dataset['dataset'], uuid.uuid4()),
            'origin_site_name': site,
            'open_for_writing': 0
        }

        return block

    def prepare_file(self, dataset, block, user, taskdir, datasetdir, stageoutdir):
        with open(os.path.join(taskdir, 'report.json')) as f:
            report = json.load(f)
        with open(os.path.join(taskdir, 'parameters.json')) as f:
            parameters = json.load(f)

        local, remote = parameters['output files'][0]
        relative = os.path.join(stageoutdir, os.path.basename(remote))

        # see https://twiki.cern.ch/twiki/bin/viewauth/CMS/DMWMPG_Namespace#store_user_and_store_temp_user
        lfn = os.path.join(datasetdir, os.path.basename(relative))

        logger.info('adding {} to block'.format(lfn))

        pfn, matched_pfn = self.match_pfn(relative, lfn)
        if not matched_pfn:
            raise ValueError(pfn)

        fileinfo = report['files']['output_info'][local]
        lumilist = []
        for run, lumis in fileinfo['runs'].items():
            lumilist += [{'run_num': str(run), 'lumi_section_num': lumi}
                         for lumi in lumis]

        file_ = {
            'adler32': fileinfo['adler32'],
            # 'auto_cross_section': 0.0,
            # 'block': block['block_name'],
            'check_sum': 'notset',
            # 'create_by': 'THE LOBSTER',  # FIXME contains an email address?
            # 'dataset': dataset['dataset'],
            'event_count': int(fileinfo['events']),
            'file_lumi_list': lumilist,
            # 'file_parent_list': [],
            'file_size': os.path.getsize(matched_pfn),
            'file_type': 'EDM',  # FIXME is this correct?
            # 'is_file_valid': 1,
            # 'last_modification_date': int(os.path.getmtime(matched_pfn)),
            # 'last_modified_by': user,  # FIXME full CN
            'logical_file_name': lfn,
        }

        return file_

    def insert_block(self, dbs, primary_dataset, dataset, user, config, basedir, datasetdir, stageoutdir, chunk):
        block = self.prepare_block(dataset, user)

        files = []
        tasks = []

        configs = []

        logger.info('preparing DBS entry for {} task block: {}'.format(len(chunk), block['block_name']))

        for task, _ in chunk:
            taskdir = os.path.join(basedir, util.id2dir(task))
            try:
                files.append(self.prepare_file(dataset, block, user, taskdir, datasetdir, stageoutdir))
                cfg = config.copy()
                cfg['lfn'] = files[-1]['logical_file_name']
                configs.append(cfg)
                tasks.append(task)
            except ValueError as e:
                logger.warn('could not find expected output for task {}: {}'.format(task, e.message))

        block.update({
            'file_count': len(files),
            'block_size': sum([int(f['file_size']) for f in files])
        })

        dump = {
            'dataset_conf_list': [config],
            'file_conf_list': configs,
            'files': files,
            'processing_era': {'processing_version': 1, 'description': 'CRAB3_processing_era'},
            'primds': primary_dataset,
            'dataset': dataset,
            'acquisition_era': {'acquisition_era_name': user, 'start_date': 0},
            'block': block,
            'file_parent_list': []
        }

        # For debugging
        # from pprint import pprint
        # pprint(config)
        try:
            dbs['local'].insertBulkBlock(dump)
        except HTTPError as e:
            if e.code in (401, 412):
                raise e
            logger.exception(e)

        return tasks, block

    def run(self, args):
        if len(args.datasets) > 0 and len(args.datasets) != len(args.workflows):
            logger.error('need the same count of datasets and workflows')
            return

        if len(args.workflows) == 0:
            args.workflows = [w.label for w in args.config.workflows]

        if len(args.datasets) == 0:
            args.datasets = [None] * len(args.workflows)

        args.config.storage.activate()
        user = args.user or self.__get_user()

        if not args.foreground:
            ttyfile = open(os.path.join(args.config.workdir, 'publish.err'), 'a')
            logger.info("saving stderr and stdout to {0}".format(
                os.path.join(args.config.workdir, 'publish.err')))

        with daemon.DaemonContext(
                detach_process=not args.foreground,
                stdout=sys.stdout if args.foreground else ttyfile,
                stderr=sys.stderr if args.foreground else ttyfile,
                files_preserve=args.preserve,
                working_directory=args.config.workdir,
                pidfile=util.get_lock(args.config.workdir)):
            db = UnitStore(args.config)

            dbs = {}
            for path, key in [[('global', 'DBSReader'), 'global'],
                              [(args.instance, 'DBSWriter'), 'local'],
                              [(args.instance, 'DBSReader'), 'reader'],
                              [(args.instance, 'DBSMigrate'), 'migrator']]:
                dbs[key] = DbsApi('https://cmsweb.cern.ch/dbs/prod/{0}/'.format(os.path.join(*path)))

            for label, dataset in zip(args.workflows, args.datasets):
                (dset, stageoutdir, release, gtag, publish_label, cfg, pset_hash, ds_id, publish_hash) = \
                    [str(x) for x in db.workflow_info(label)]

                if dataset is None and not dset.startswith('/'):
                    logger.error('invalid dataset "{}", please specify a primary dataset manually'.format(dset))
                    continue

                dset = (dataset if dataset else dset).strip('/').split('/')[0]

                if label == publish_label:
                    publish_label = '{}_{}'.format(args.config.label.split('_', 1)[1],
                                                   publish_label)

                if not pset_hash or pset_hash == 'None':
                    try:
                        pset_hash = hash_pset(cfg, label, args)
                        db.update_pset_hash(pset_hash, label)
                    except Exception:
                        logger.warning('error calculating the cmssw parameter set hash')

                # block = BlockDump(user, dset, dbs['global'], publish_hash, publish_label, release, pset_hash, gtag)
                primary_dataset, dataset = self.insert_dataset(dbs, dset, user, publish_label, publish_hash, args.version)
                tasks = list(db.successful_tasks(label))
                logger.info('found {} successful {} tasks to publish'.format(len(tasks), label))

                first_task = 0
                inserted = []
                basedir = os.path.join(args.config.workdir, label, 'successful')
                datasetdir = os.path.join('/store/user', user, dset, publish_label + '_' + publish_hash)

                config = self.__get_config(args, label, pset_hash)

                while first_task < len(tasks):
                    chunk = tasks[first_task:first_task + args.block_size]
                    processed, block = self.insert_block(dbs, primary_dataset, dataset, user, config, basedir, datasetdir, stageoutdir, chunk)
                    inserted += processed
                    first_task += args.block_size

                    if len(processed) > 0:
                        db.update_published(label, processed, block['block_name'])

                missing = list(set(t for (t, _) in tasks) - set(inserted))
                if len(missing) > 0:
                    template = "the following task(s) have not been published because their output could not be found: {0}"
                    logger.warning(template.format(", ".join(map(str, missing))))
