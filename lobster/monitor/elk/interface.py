import elasticsearch as es
import elasticsearch_dsl as es_dsl
from datetime import datetime
import json
import re
import inspect
import logging
import os
import requests

from lobster.util import Configurable, PartiallyMutable
import lobster

logger = logging.getLogger('lobster.monitor.elk')


class ElkInterface(Configurable):

    """
    Enables ELK stack monitoring for the current Lobster run using an existing
    Elasticsearch cluster.

    Attributs modifiable at runtime:
    * `es_host`
    * `es_port`
    * `kib_host`
    * `kib_port`
    * `dashboards`
    * `refresh_interval`

    Parameters
    ----------
        es_host : str
            Host running Elasticsearch cluster.
        es_port : int
            Port number running Elasticsearch HTTP service.
        kib_host : str
            Host running Kibana instance connected to Elasticsearch cluster.
        kib_port : int
            Port number running Kibana HTTP service.
        user : str
            User ID to label Elasticsearch indices and Kibana objects.
        project : str
            Project ID to label Elasticsearch indices and Kibana objects.
        dashboards : list
            List of dashboards to include from the Kibana templates. Defaults
            to including only the core dashboard. Available dashboards: Core,
            Advanced, Tasks.
        refresh_interval : int
            Refresh interval for Kibana dashboards, in seconds. Defaults to
            300 seconds = 5 minutes.
    """
    _mutable = {'es_host': ('config.elk.update_client', [], False),
                'es_port': ('config.elk.update_client', [], False),
                'kib_host': ('config.elk.update_kibana', [], False),
                'kib_port': ('config.elk.update_kibana', [], False),
                'dashboards': ('config.elk.update_kibana', [], False),
                'refresh_interval': ('config.elk.update_links', [], False)}

    def __init__(self, es_host, es_port, kib_host, kib_port, project,
                 dashboards=None, refresh_interval=30):
        self.es_host = es_host
        self.es_port = es_port
        self.kib_host = kib_host
        self.kib_port = kib_port
        self.user = os.environ['USER']
        self.project = project
        self.dashboards = dashboards or ['Core']
        self.refresh_interval = refresh_interval
        self.prefix = '[' + self.user + '_' + self.project + ']'
        self.categories = []
        self.start_time = None
        self.end_time = None
        self.previous_stats = {}
        self.template_dir = os.path.join(os.path.dirname(
            os.path.abspath(lobster.__file__)), 'monitor', 'elk', 'data')
        self.client = es.Elasticsearch([{'host': self.es_host,
                                         'port': self.es_port}])

        # FIXME: supposed to check that the Elasticsearch client exists
        # and cause the config file to be rejected if it doesn't
        # but breaks lobster with an sqlite3 error in unit.py
        # so we check in self.create() instead, which fails more quietly
        #
        # self.check_client()

    def __getstate__(self):
        state = dict(self.__dict__)
        del state['client']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

        with PartiallyMutable.unlock():
            self.client = es.Elasticsearch([{'host': self.es_host,
                                             'port': self.es_port}])

    def create(self, categories):
        with PartiallyMutable.unlock():
            self.start_time = datetime.utcnow()
            self.categories = categories

        logger.info("checking Elasticsearch client")

        self.check_client()

        try:
            indices = self.client.indices.get_aliases().keys()
        except es.exceptions.ElasticsearchException as e:
            logger.error(e)
            return

        if any(self.prefix in s for s in indices):
            logger.info("Elasticsearch indices with prefix " + self.prefix +
                        " already exist")
            self.delete_elasticsearch()

        self.update_kibana()
        logger.info("beginning ELK monitoring")

    def end(self):
        logger.info("ending ELK monitoring")
        with PartiallyMutable.unlock():
            self.end_time = datetime.utcnow()
        self.update_links()

    def resume(self):
        logger.info("resuming ELK monitoring")
        with PartiallyMutable.unlock():
            self.end_time = None
        self.update_links()

    def cleanup(self):
        self.delete_kibana()
        self.delete_elasticsearch()

    def check_client(self):
        try:
            logger.info("cluster health: " + self.client.cat.health())
        except es.exceptions.ElasticsearchException as e:
            logger.error(e)
            raise AttributeError("could not connect to Elasticsearch cluster" +
                                 " {0}:{1}".format(self.es_host, self.es_port))

    def update_client(self):
        with PartiallyMutable.unlock():
            self.client = es.Elasticsearch([{'host': self.es_host,
                                             'port': self.es_port}])

    def download_templates(self):
        logger.info("getting Kibana objects with prefix " + self.prefix)

        logger.info("getting index patterns")
        index_dir = os.path.join(self.template_dir, 'index')
        try:
            try:
                os.mkdir(self.template_dir)
                os.mkdir(os.path.join(self.template_dir, 'index'))
            except OSError:
                pass

            search_index = es_dsl.Search(using=self.client, index='.kibana') \
                .filter('prefix', _id=self.prefix) \
                .filter('match', _type='index-pattern') \
                .extra(size=10000)
            response_index = search_index.execute()

            for index in response_index:
                index.meta.id = index.meta.id \
                    .replace(self.prefix, '[template]')
                index.title = index.title.replace(self.prefix, '[template]')

                with open(os.path.join(index_dir, index.meta.id) + '.json',
                          'w') as f:
                    f.write(json.dumps(index.to_dict(), indent=4,
                                       sort_keys=True))
                    f.write('\n')
        except Exception as e:
            logger.error(e)

        dash_dir = os.path.join(self.template_dir, 'dash')
        for name in self.dashboards:
            logger.info("getting " + name + " dashboard")
            vis_ids = []
            try:
                try:
                    os.mkdir(os.path.join(dash_dir))
                except OSError:
                    pass

                search_dash = es_dsl.Search(
                    using=self.client, index='.kibana') \
                    .filter('prefix', _id=self.prefix + '-' + name) \
                    .filter('match', _type='dashboard') \
                    .extra(size=10000)
                response_dash = search_dash.execute()

                for dash in response_dash:
                    dash.meta.id = dash.meta.id \
                        .replace(self.prefix, '[template]')
                    dash.title = dash.title.replace(self.prefix, '[template]')

                    dash_panels = json.loads(dash.panelsJSON)
                    for panel in dash_panels:
                        vis_ids.append(panel['id'])
                        panel['id'] = panel['id'].replace(
                            self.prefix, '[template]')
                    dash.panelsJSON = json.dumps(dash_panels, sort_keys=True)

                    with open(os.path.join(dash_dir, dash.meta.id) + '.json',
                              'w') as f:
                        f.write(json.dumps(dash.to_dict(), indent=4,
                                           sort_keys=True))
                        f.write('\n')
            except Exception as e:
                logger.error(e)

            logger.info("getting " + name + " visualizations")
            vis_dir = os.path.join(self.template_dir, 'vis')
            try:
                os.mkdir(vis_dir)
            except OSError:
                pass
            for vis_id in vis_ids:
                try:
                    search_vis = es_dsl.Search(
                        using=self.client, index='.kibana') \
                        .filter('match', _id=vis_id) \
                        .filter('match', _type='visualization')
                    response_vis = search_vis.execute()

                    for vis in response_vis:
                        vis.meta.id = vis.meta.id \
                            .replace(self.prefix, '[template]')
                        vis.title = vis.title \
                            .replace(self.prefix, '[template]')

                        vis_state = json.loads(vis['visState'])
                        vis_state['title'] = vis['title']

                        if vis_state['type'] == 'markdown':
                            vis_state['params']['markdown'] = "text goes here"
                        else:
                            source = json.loads(
                                vis.kibanaSavedObjectMeta.searchSourceJSON)
                            source['index'] = source['index'].replace(
                                self.prefix, '[template]')
                            vis.kibanaSavedObjectMeta.searchSourceJSON = \
                                json.dumps(source, sort_keys=True)

                        vis['visState'] = json.dumps(vis_state, sort_keys=True)

                        with open(os.path.join(vis_dir, vis.meta.id) +
                                  '.json', 'w') as f:
                            f.write(json.dumps(vis.to_dict(), indent=4,
                                               sort_keys=True))
                            f.write('\n')
                except Exception as e:
                    logger.error(e)

    def update_kibana(self):
        logger.info("generating Kibana objects from templates")

        logger.debug("generating index patterns")
        try:
            index_dir = os.path.join(self.template_dir, 'index')
            for index_path in os.listdir(index_dir):
                with open(os.path.join(index_dir, index_path)) as f:
                    index = json.load(f)

                index['title'] = index['title'] \
                    .replace('[template]', self.prefix)

                index_id = index_path.replace('[template]', self.prefix) \
                    .replace('.json', '')

                self.client.index(index='.kibana', doc_type='index-pattern',
                                  id=index_id, body=index)
        except Exception as e:
            logger.error(e)

        for name in self.dashboards:
            logger.debug("generating " + name + " dashboard")
            vis_paths = []
            try:
                dash_dir = os.path.join(self.template_dir, 'dash')
                dash_path = '[template]-{}.json'.format(name)
                with open(os.path.join(dash_dir, dash_path)) as f:
                    dash = json.load(f)

                dash['title'] = dash['title'] \
                    .replace('[template]', self.prefix)

                dash_panels = json.loads(dash['panelsJSON'])
                for panel in dash_panels:
                    vis_paths.append(panel['id'] + '.json')
                    panel['id'] = panel['id'] \
                        .replace('[template]', self.prefix)
                dash['panelsJSON'] = json.dumps(dash_panels, sort_keys=True)

                dash_id = dash_path.replace('[template]', self.prefix)[:-5]

                self.client.index(index='.kibana', doc_type='dashboard',
                                  id=dash_id, body=dash)
            except Exception as e:
                logger.error(e)

            logger.debug("generating " + name + " visualizations")
            vis_dir = os.path.join(self.template_dir, 'vis')
            for vis_path in vis_paths:
                try:
                    with open(os.path.join(vis_dir, vis_path)) as f:
                        vis = json.load(f)

                    vis['title'] = vis['title'] \
                        .replace('[template]', self.prefix)

                    vis_state = json.loads(vis['visState'])

                    if not vis_state['type'] == 'markdown':
                        source = json.loads(
                            vis['kibanaSavedObjectMeta']['searchSourceJSON'])

                        source['index'] = source['index'] \
                            .replace('[template]', self.prefix)
                        vis['kibanaSavedObjectMeta']['searchSourceJSON'] = \
                            json.dumps(source, sort_keys=True)

                    vis_id = \
                        vis_path.replace('[template]', self.prefix)[:-5]

                    self.client.index(index='.kibana',
                                      doc_type='visualization',
                                      id=vis_id, body=vis)
                except Exception as e:
                    logger.error(e)

        self.update_links()

    def update_links(self):
        logger.debug("generating dashboard links")
        dash_links = {}
        try:
            link_prefix = "http://{0}:{1}/app/" \
                .format(self.kib_host, self.kib_port)

            if self.end_time:
                time_filter = requests.utils.quote(
                    ("_g=(refreshInterval:" +
                     "(display:Off,pause:!f,section:0,value:0),time:" +
                     "(from:'{0}Z',mode:absolute,to:'{1}Z'))")
                    .format(self.start_time, self.end_time),
                    safe='/:!?,&=#')
            else:
                time_filter = requests.utils.quote(
                    ("_g=(refreshInterval:" +
                     "(display:'{0} seconds',pause:!f,section:2," +
                     "value:{1}),time:(from:'{2}Z',mode:absolute,to:now))")
                    .format(self.refresh_interval,
                            int(self.refresh_interval * 1e3),
                            self.start_time),
                    safe='/:!?,&=#')

            dash_dir = os.path.join(self.template_dir, 'dash')
            for name in self.dashboards:
                dash_path = '[template]-{}.json'.format(name)
                with open(os.path.join(dash_dir, dash_path)) as f:
                    dash = json.load(f)

                dash_id = dash_path.replace('[template]', self.prefix)[:-5]
                dash['title'] = dash['title'] \
                    .replace('[template]', self.prefix)

                link = requests.utils.quote(
                    "kibana#/dashboard/{0}".format(dash_id),
                    safe='/:!?,&=#')
                logger.info("Kibana {0} dashboard at {1}{2}?{3}"
                            .format(name, link_prefix, link, time_filter))
                dash_links[name] = link

        except Exception as e:
            logger.error(e)

        logger.debug("generating shared link widget")
        try:
            shared_links_text = "####Dashboards\n" \
                .format(self.user, self.project)

            for name in dash_links:
                shared_links_text += "- [{0}]({1})\n" \
                    .format(name, dash_links[name])

            task_log_link = requests.utils.quote(
                ("kibana#/discover?_a=(columns:" +
                 "!(Task.id,TaskUpdate.exit_code,Task.log),index:" +
                 "{0}_lobster_tasks,interval:auto,query:(query_string:" +
                 "(analyze_wildcard:!t,query:'!!TaskUpdate.exit_code:0'" +
                 ")),sort:!(_score,desc))")
                .format(self.prefix),
                safe='/:!?,&=#')
            shared_links_text += "\n####[Failed task logs]({0})\n" \
                .format(task_log_link)

            with open(os.path.join(self.template_dir, 'vis',
                                   '[template]-Links') + '.json',
                      'r') as f:
                shared_links_vis = json.load(f)

            shared_links_vis['title'] = shared_links_vis['title'].replace(
                '[template]', self.prefix)

            shared_links_state = json.loads(shared_links_vis['visState'])
            shared_links_state['params']['markdown'] = shared_links_text
            shared_links_vis['visState'] = json.dumps(shared_links_state,
                                                      sort_keys=True)

            self.client.index(index='.kibana', doc_type='visualization',
                              id=self.prefix + "-Links",
                              body=shared_links_vis)
        except Exception as e:
            logger.error(e)

        for name in dash_links:
            logger.debug("generating " + name + " link widget")
            try:
                links_text = "####Category filters\n".format(name)

                all_filter = requests.utils.quote(
                    "&_a=(query:(query_string:(analyze_wildcard:!t,query:" +
                    "'(_missing_:category) OR category:all')))",
                    safe='/:!?,&=#')
                links_text += "- [all]({0}?{1})\n" \
                    .format(dash_links[name], all_filter)

                merge_filter = requests.utils.quote(
                    "&_a=(query:(query_string:(analyze_wildcard:!t,query:" +
                    "'(_missing_:Task.category) OR Task.category:merge')))",
                    safe='/:!?,&=#')
                links_text += "- [merge]({0}?{1})\n" \
                    .format(dash_links[name], merge_filter)

                for category in self.categories:
                    cat_filter = requests.utils.quote(
                        ("_a=(query:(query_string:(analyze_wildcard:!t," +
                         "query:'(_missing_:Task.category AND " +
                         "_missing_:TaskUpdate AND _missing_:category) " +
                         "OR category:{0} OR Task.category:{0}')))")
                        .format(category), safe='/:!?,&=#')

                    links_text += "- [{0}]({1}?{2})\n" \
                        .format(category, dash_links[name], cat_filter)

                links_text += "\n####[Reset time range]({0}?{1})\n" \
                    .format(dash_links[name], time_filter)

                with open(os.path.join(self.template_dir, 'vis',
                                       '[template]-{0}-links'
                                       .format(name)) + '.json',
                          'r') as f:
                    links_vis = json.load(f)

                links_vis['title'] = links_vis['title'].replace(
                    '[template]', self.prefix)

                links_state = json.loads(links_vis['visState'])
                links_state['params']['markdown'] = links_text
                links_vis['visState'] = json.dumps(links_state,
                                                   sort_keys=True)

                self.client.index(index='.kibana', doc_type='visualization',
                                  id='{0}-{1}-links'
                                  .format(self.prefix, name),
                                  body=links_vis)
            except Exception as e:
                logger.error(e)

    def delete_kibana(self):
        logger.info("deleting Kibana objects with prefix " + self.prefix)
        try:
            search = es_dsl.Search(using=self.client, index='.kibana') \
                .filter('prefix', _id=self.prefix) \
                .extra(size=10000)
            response = search.execute()

            for result in response:
                self.client.delete(index='.kibana',
                                   doc_type=result.meta.doc_type,
                                   id=result.meta.id)
        except Exception as e:
            logger.error(e)

    def delete_elasticsearch(self):
        logger.info("deleting Elasticsearch indices with prefix " +
                    self.prefix)
        try:
            self.client.indices.delete(index=self.prefix + '*')
        except es.exceptions.ElasticsearchException as e:
            logger.error(e)

    def dictify(self, thing, skip=None):
        thing = dict([(m, o) for (m, o) in inspect.getmembers(thing)
                      if not inspect.isroutine(o) and not m.startswith('__')])

        if isinstance(skip, basestring):
            try:
                thing.pop(skip)
            except KeyError:
                pass
        else:
            for key in skip:
                try:
                    thing.pop(key)
                except KeyError:
                    pass

        return thing

    def index_task(self, task):
        logger.debug("parsing Task object")
        try:
            task = self.dictify(task, skip=('_task'))

            task['resources_requested'] = self.dictify(
                task['resources_requested'], skip=('this'))
            task['resources_measured'] = self.dictify(
                task['resources_measured'], skip=('this', 'peak_times'))
            task['resources_allocated'] = self.dictify(
                task['resources_allocated'], skip=('this'))
        except Exception as e:
            logger.error(e)
            return

        logger.debug("parsing Task timestamps")
        try:
            task['resources_measured']['cpu_wall_ratio'] = \
                task['resources_measured']['cpu_time'] / \
                float(task['resources_measured']['wall_time'])

            task['send_input_start'] = datetime.utcfromtimestamp(
                float(str(task['send_input_start'])[:10]))
            task['send_input_finish'] = datetime.utcfromtimestamp(
                float(str(task['send_input_finish'])[:10]))
            task['execute_cmd_start'] = datetime.utcfromtimestamp(
                float(str(task['execute_cmd_start'])[:10]))
            task['execute_cmd_finish'] = datetime.utcfromtimestamp(
                float(str(task['execute_cmd_finish'])[:10]))
            task['receive_output_start'] = datetime.utcfromtimestamp(
                float(str(task['receive_output_start'])[:10]))
            task['receive_output_finish'] = datetime.utcfromtimestamp(
                float(str(task['receive_output_finish'])[:10]))
            task['submit_time'] = datetime.utcfromtimestamp(
                float(str(task['submit_time'])[:10]))
            task['finish_time'] = datetime.utcfromtimestamp(
                float(str(task['finish_time'])[:10]))

            task['resources_measured']['start'] = datetime.utcfromtimestamp(
                float(str(task['resources_measured']['start'])[:10]))
            task['resources_measured']['end'] = datetime.utcfromtimestamp(
                float(str(task['resources_measured']['end'])[:10]))
        except Exception as e:
            logger.error(e)

        # FIXME: doesn't appear to work
        logger.debug("parsing Task log")
        try:
            task_log = task.pop('output')
            task['log'] = requests.utils.quote(
                ("kibana#/doc/{0}_lobster_tasks/" +
                 "{0}_lobster_task_logs/log?id={1}")
                .format(self.prefix, task['id']),
                safe='/:!?,&=#')

            e_p = re.compile(
                r"Begin Fatal Exception([\s\S]*)End Fatal Exception")
            e_match = e_p.search(task_log)

            if e_match:
                logger.debug("parsing fatal exception")

                task['fatal_exception'] = {}
                task['fatal_exception']['message'] = e_match.group(1)

                e_cat_p = re.compile(r"'(.*)'")
                task['fatal_exception']['exception_category'] = \
                    e_cat_p.search(task['fatal_exception']['message']).group(1)

                e_mess_p = re.compile(r"Exception Message:\n(.*)")
                task['fatal_exception']['exception_message'] = \
                    e_mess_p.search(task['fatal_exception']['message'])\
                    .group(1)
        except Exception as e:
            logger.error(e)

        logger.debug("sending Task documents to Elasticsearch")
        try:
            task_doc = {'doc': {'Task': task},
                        'doc_as_upsert': True}
            self.client.update(index=self.prefix + '_lobster_tasks',
                               doc_type='task', id=task['id'],
                               body=task_doc)

            log_doc = {'text': task_log}
            self.client.index(index=self.prefix + '_lobster_task_logs',
                              doc_type='log', id=task['id'],
                              body=log_doc)
        except Exception as e:
            logger.error(e)

    def index_task_update(self, task_update):
        logger.debug("parsing TaskUpdate object")
        try:
            task_update = dict(task_update.__dict__)

            task_update['megabytes_output'] = \
                task_update['bytes_output'] / 1024.0**2

            task_update['allocated_disk_MB'] = \
                task_update['allocated_disk'] / 1024.0

            task_update['allocated_memory_MB'] = \
                task_update['allocated_memory'] / 1024.0

            task_update['bandwidth'] = \
                task_update['network_bytes_received'] / 1e6 / \
                task_update['time_on_worker']

            task_update['percent_efficiency'] = \
                task_update['time_cpu'] * 100 / \
                (1. * task_update['cores'] *
                 (task_update['time_processing_end'] -
                  task_update['time_prologue_end']))

            status_code_map = {
                0: 'initialized',
                1: 'assigned',
                2: 'successful',
                3: 'failed',
                4: 'aborted',
                6: 'published',
                7: 'merging',
                8: 'merged'
            }
            task_update['status_text'] = status_code_map[task_update['status']]

            cache_map = {0: 'cold cache', 1: 'hot cache', 2: 'dedicated'}
            task_update['cache_text'] = cache_map[task_update['cache']]

        except Exception as e:
            logger.error(e)
            return

        logger.debug("calculating TaskUpdate time intervals")
        try:
            task_update['runtime'] = \
                task_update['time_processing_end'] - \
                task_update['time_wrapper_start']
            task_update['time_input_transfer'] = \
                task_update['time_transfer_in_start'] - \
                task_update['time_transfer_in_end']
            task_update['time_startup'] = \
                task_update['time_wrapper_start'] - \
                task_update['time_transfer_in_end']
            task_update['time_release_setup'] = \
                task_update['time_wrapper_ready'] - \
                task_update['time_wrapper_start']
            task_update['time_stage_in'] = \
                task_update['time_stage_in_end'] - \
                task_update['time_wrapper_ready']
            task_update['time_prologue'] = \
                task_update['time_prologue_end'] - \
                task_update['time_stage_in_end']
            task_update['time_overhead'] = \
                task_update['time_wrapper_ready'] - \
                task_update['time_wrapper_start']
            task_update['time_executable'] = \
                task_update['time_processing_end'] - \
                task_update['time_prologue_end']
            task_update['time_epilogue'] = \
                task_update['time_epilogue_end'] - \
                task_update['time_processing_end']
            task_update['time_stage_out'] = \
                task_update['time_stage_out_end'] - \
                task_update['time_epilogue_end']
            task_update['time_output_transfer_wait'] = \
                task_update['time_transfer_out_start'] - \
                task_update['time_stage_out_end']
            task_update['time_output_transfer_work_queue'] = \
                task_update['time_transfer_out_end'] - \
                task_update['time_transfer_out_start']

            task_update['time_total_eviction'] = \
                task_update['time_total_on_worker'] - \
                task_update['time_on_worker']

            if task_update['exit_code'] == 0:
                task_update['time_total_overhead'] = \
                    task_update['time_prologue_end'] - \
                    task_update['time_transfer_in_start']
                task_update['time_total_processing'] = \
                    task_update['time_processing_end'] - \
                    task_update['time_prologue_end']
                task_update['time_total_stage_out'] = \
                    task_update['time_transfer_out_end'] - \
                    task_update['time_processing_end']
            else:
                task_update['time_total_failed'] = \
                    task_update['time_total_on_worker']
        except Exception as e:
            logger.error(e)

        logger.debug("parsing TaskUpdate timestamps")
        try:
            task_update['time_processing_end'] = datetime.utcfromtimestamp(
                task_update['time_processing_end'])
            task_update['time_prologue_end'] = datetime.utcfromtimestamp(
                task_update['time_prologue_end'])
            task_update['time_retrieved'] = datetime.utcfromtimestamp(
                task_update['time_retrieved'])
            task_update['time_stage_in_end'] = datetime.utcfromtimestamp(
                task_update['time_stage_in_end'])
            task_update['time_stage_out_end'] = datetime.utcfromtimestamp(
                task_update['time_stage_out_end'])
            task_update['time_transfer_in_end'] = datetime.utcfromtimestamp(
                task_update['time_transfer_in_end'])
            task_update['time_transfer_in_start'] = datetime.utcfromtimestamp(
                task_update['time_transfer_in_start'])
            task_update['time_transfer_out_end'] = datetime.utcfromtimestamp(
                task_update['time_transfer_out_end'])
            task_update['time_transfer_out_start'] = datetime.utcfromtimestamp(
                task_update['time_transfer_out_start'])
            task_update['time_wrapper_ready'] = datetime.utcfromtimestamp(
                task_update['time_wrapper_ready'])
            task_update['time_wrapper_start'] = datetime.utcfromtimestamp(
                task_update['time_wrapper_start'])
            task_update['time_epilogue_end'] = datetime.utcfromtimestamp(
                task_update['time_epilogue_end'])
        except Exception as e:
            logger.error(e)

        logger.debug("sending TaskUpdate document to Elasticsearch")
        try:
            doc = {'doc': {'TaskUpdate': task_update,
                           'timestamp': task_update['time_retrieved']},
                   'doc_as_upsert': True}
            self.client.update(index=self.prefix + '_lobster_tasks',
                               doc_type='task', id=task_update['id'],
                               body=doc)
        except Exception as e:
            logger.error(e)

    def index_stats(self, now, left, times, log_attributes, stats, category):
        logger.debug("parsing lobster stats log")
        try:
            keys = ['timestamp', 'units_left'] + \
                ['total_{}_time'.format(k) for k in sorted(times.keys())] + \
                log_attributes + ['category']

            values = \
                [datetime.utcfromtimestamp(int(now.strftime('%s'))), left] + \
                [times[k] for k in sorted(times.keys())] + \
                [getattr(stats, a) for a in log_attributes] + [category]

            stats = dict(zip(keys, values))

            stats['committed_memory_GB'] = stats['committed_memory'] / 1024.0
            stats['total_memory_GB'] = stats['total_memory'] / 1024.0

            stats['committed_disk_GB'] = stats['committed_disk'] / 1024.0
            stats['total_disk_GB'] = stats['total_disk'] / 1024.0

            stats['start_time'] = datetime.utcfromtimestamp(
                float(str(stats['start_time'])[:10]))
            stats['time_when_started'] = datetime.utcfromtimestamp(
                float(str(stats['time_when_started'])[:10]))

            if category not in self.previous_stats:
                self.previous_stats[category] = {}

            if 'timestamp' in self.previous_stats[category]:
                stats['time_diff'] = \
                    max(int(stats['timestamp'].strftime('%s')) * 10e6 -
                        self.previous_stats[category]['timestamp'], 0)

                stats['time_other_lobster'] = \
                    max(stats['time_diff'] -
                        stats['total_status_time'] -
                        stats['total_create_time'] -
                        stats['total_action_time'] -
                        stats['total_update_time'] -
                        stats['total_fetch_time'] -
                        stats['total_return_time'], 0)

                stats['time_other_wq'] = \
                    max(stats['time_diff'] - stats['time_send'] -
                        stats['time_receive'] - stats['time_status_msgs'] -
                        stats['time_internal'] - stats['time_polling'] -
                        stats['time_application'], 0)

                stats['time_idle'] = \
                    stats['time_diff'] * stats['idle_percentage']

            self.previous_stats[category]['timestamp'] = \
                int(stats['timestamp'].strftime('%s')) * 10e6

            for key in stats.keys():
                if key.startswith('workers_'):
                    if key in self.previous_stats[category]:
                        stats['new_' + key] = \
                            max(stats[key] -
                                self.previous_stats[category][key], 0)
                    self.previous_stats[category][key] = stats[key]
        except Exception as e:
            logger.error(e)
            return

        logger.debug("sending lobster stats document to Elasticsearch")

        try:
            self.client.index(index=self.prefix + '_lobster_stats',
                              doc_type='log', body=stats,
                              id=str(int(int(now.strftime('%s')) * 1e6 +
                                         now.microsecond)))
        except Exception as e:
            logger.error(e)
