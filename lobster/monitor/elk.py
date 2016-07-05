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

logger = logging.getLogger('lobster.monitor.elk')


class ElkInterface(Configurable):
    """
    Enables ELK stack monitoring for the current Lobster run using an existing
    Elasticsearch instance.

    Attributs modifiable at runtime:
    * `es_host`
    * `es_port`
    * `kib_host`
    * `kib_port`
    * `modules`

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
        populate_template : bool
            Whether to send documents to indices with the [template] prefix,
            in addition to sending them to the [user_project] prefix. Defaults
            to false.
        modules : list
            List of modules to include from the Kibana templates. Defaults to
            including only the core templates.
    """
    _mutable = {'es_host': ('config.elk.update_client', [], False),
                'es_port': ('config.elk.update_client', [], False),
                'kib_host': ('config.elk.update_kibana', [], False),
                'kib_port': ('config.elk.update_kibana', [], False),
                'modules': ('config.elk.update_kibana', [], False)}

    def __init__(self, es_host, es_port, kib_host, kib_port, project,
                 populate_template=False, modules=None):
        self.es_host = es_host
        self.es_port = es_port
        self.kib_host = kib_host
        self.kib_port = kib_port
        self.user = os.environ['USER']
        self.project = project
        self.populate_template = populate_template
        self.modules = modules or ['core']
        self.prefix = '[' + self.user + '_' + self.project + ']'
        self.start_time = datetime.utcnow()
        self.end_time = None
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
            self.end_time = None

        self.update_links()

    def create(self):
        logger.info("checking Elasticsearch indices")

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

    def cleanup(self):
        self.delete_kibana()
        self.delete_elasticsearch()

    def end(self):
        logger.info("ending ELK monitoring")

        with PartiallyMutable.unlock():
            self.end_time = datetime.utcnow()

        self.update_links()

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

    def update_kibana(self):
        logger.info("generating Kibana objects from templates")

        any_prefix = re.compile('\[.*\]')

        logger.debug("generating index patterns")
        try:
            search_index = es_dsl.Search(using=self.client, index='.kibana') \
                .filter('prefix', _id='[template]') \
                .filter('match', _type='index-pattern')
            response_index = search_index.execute()

            for index in response_index:
                index.meta.id = index.meta.id.replace(
                    '[template]', self.prefix)
                index.title = index.title.replace('[template]', self.prefix)
                self.client.index(index='.kibana',
                                  doc_type=index.meta.doc_type,
                                  id=index.meta.id, body=index.to_dict())
        except Exception as e:
            logger.error(e)

        for module in self.modules:
            logger.debug("generating " + module + " visualizations")
            try:
                search_vis = es_dsl.Search(
                    using=self.client, index='.kibana') \
                    .filter('prefix', _id='[template][' + module + ']') \
                    .filter('match', _type='visualization')

                response_vis = search_vis.execute()

                for vis in response_vis:
                    vis.meta.id = vis.meta.id.replace(
                        '[template]', self.prefix)
                    vis.title = vis.title.replace('[template]', self.prefix)

                    source = json.loads(
                        vis.kibanaSavedObjectMeta.searchSourceJSON)
                    source['index'] = any_prefix.sub(
                        self.prefix, source['index'])
                    vis.kibanaSavedObjectMeta.searchSourceJSON = json.dumps(
                        source)

                    self.client.index(index='.kibana',
                                      doc_type=vis.meta.doc_type,
                                      id=vis.meta.id, body=vis.to_dict())
            except Exception as e:
                logger.error(e)

            logger.debug("generating " + module + " dashboard")
            try:
                search_dash = es_dsl.Search(
                    using=self.client, index='.kibana') \
                    .filter('prefix', _id='[template][' + module + ']') \
                    .filter('match', _type='dashboard')

                response_dash = search_dash.execute()

                for dash in response_dash:
                    dash.meta.id = dash.meta.id.replace(
                        '[template]', self.prefix)
                    dash.title = dash.title.replace('[template]', self.prefix)

                    dash_panels = json.loads(dash.panelsJSON)
                    for panel in dash_panels:
                        panel['id'] = panel['id'].replace(
                            '[template]', self.prefix)
                    dash.panelsJSON = json.dumps(dash_panels)

                    self.client.index(index='.kibana',
                                      doc_type=dash.meta.doc_type,
                                      id=dash.meta.id, body=dash.to_dict())
            except Exception as e:
                logger.error(e)

        self.update_links()

    def update_links(self):
        logger.debug("generating dashboard link widget")
        links_text = "###{0}'s {1} dashboards\n" \
            .format(self.user, self.project)
        try:
            for module in self.modules:
                search_dash = es_dsl.Search(
                    using=self.client, index='.kibana') \
                    .filter('prefix', _id=self.prefix + '[' + module + ']') \
                    .filter('match', _type='dashboard')

                response_dash = search_dash.execute()

                for dash in response_dash:
                    if self.end_time:
                        link = requests.utils.quote(
                            ("http://{0}:{1}/app/kibana#/dashboard/{2}" +
                             "?_g=(refreshInterval:(display:Off,pause:!f," +
                             "section:0,value:0),time:(from:'{3}Z',mode:" +
                             "absolute,to:'{4}Z'))")
                            .format(self.kib_host, self.kib_port, dash.meta.id,
                                    self.start_time, self.end_time),
                            safe='/:!?,=#')
                    else:
                        link = requests.utils.quote(
                            ("http://{0}:{1}/app/kibana#/dashboard/{2}" +
                             "?_g=(refreshInterval:(display:'5 minutes'" +
                             ",pause:!f,section:2,value:900000),time:" +
                             "(from:'{3}Z',mode:absolute,to:now))")
                            .format(self.kib_host, self.kib_port, dash.meta.id,
                                    self.start_time),
                            safe='/:!?,=#')

                    logger.info("Kibana " + module + " dashboard at " + link)

                    links_text += "- [" + dash.title + "](" + link + ")\n"

            links_vis = self.client.get(index='.kibana',
                                        doc_type='visualization',
                                        id='[template]-Links')['_source']

            links_vis['title'] = links_vis['title'].replace(
                '[template]', self.prefix)

            links_state = json.loads(links_vis['visState'])
            links_state['params']['markdown'] = links_text
            links_vis['visState'] = json.dumps(links_state)

            self.client.index(index='.kibana', doc_type='visualization',
                              id=self.prefix + "-Links", body=links_vis)
        except Exception as e:
            logger.error(e)

    def delete_kibana(self):
        logger.info("deleting Kibana objects with prefix " + self.prefix)
        try:
            search = es_dsl.Search(using=self.client, index='.kibana') \
                .filter('prefix', _id=self.prefix)
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
            self.client.indices.delete(index=self.prefix + '_*')
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
                logger.debug(key)
                try:
                    thing.pop(key)
                except KeyError:
                    pass

        return thing

    def index_task(self, task):
        logger.debug("parsing Task object")
        try:
            task = self.dictify(task, skip=('_task', 'command'))

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

        logger.debug("checking Task for fatal exception")
        try:
            task_log = task.pop('output')

            e_p = re.compile(
                r"Begin Fatal Exception([\s\S]*)End Fatal Exception")
            e_match = e_p.search(task_log)

            if e_match:
                logger.debug("parsing fatal exception")

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

        logger.debug("sending Task document to Elasticsearch")
        try:
            upsert_doc = {'doc': {'Task': task,
                                  'timestamp': task['submit_time']},
                          'doc_as_upsert': True}
            self.client.update(index=self.prefix + '_lobster_tasks',
                               doc_type='task', id=task['id'],
                               body=upsert_doc)
            if self.populate_template:
                self.client.update(index='[template]_lobster_tasks',
                                   doc_type='task', id=task['id'],
                                   body=upsert_doc)
        except Exception as e:
            logger.error(e)

    def index_task_update(self, task_update):
        logger.debug("parsing TaskUpdate object")
        try:
            task_update = dict(task_update.__dict__)
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

            task_update['time_total_eviction_execution'] = \
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
            upsert_doc = {'doc': {'TaskUpdate': task_update},
                          'doc_as_upsert': True}
            self.client.update(index=self.prefix + '_lobster_tasks',
                               doc_type='task', id=task_update['id'],
                               body=upsert_doc)
            if self.populate_template:
                self.client.update(index='[template]_lobster_tasks',
                                   doc_type='task', id=task_update['id'],
                                   body=upsert_doc)
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
        except Exception as e:
            logger.error(e)
            return

        logger.debug("sending lobster stats document to Elasticsearch")

        try:
            self.client.index(index=self.prefix + '_lobster_stats',
                              doc_type='log', body=stats,
                              id=str(int(int(now.strftime('%s')) * 1e6 +
                                         now.microsecond)))
            if self.populate_template:
                self.client.index(index='[template]_lobster_stats',
                                  doc_type='log', body=stats,
                                  id=str(int(int(now.strftime('%s')) * 1e6 +
                                             now.microsecond)))
        except Exception as e:
            logger.error(e)
