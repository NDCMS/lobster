import elasticsearch as es
import elasticsearch_dsl as es_dsl
from datetime import datetime
import json
import re
import inspect
import logging
import os
import requests

from lobster.util import Configurable

logger = logging.getLogger('lobster.monitor.elk')


class ElkInterface(Configurable):
    """
    Enables ELK stack monitoring for the current Lobster run using an existing
    Elasticsearch instance.

    Parameters
    ----------
        es_host : str
            Host running Elasticsearch cluster.
        es_port : int
            Port number running Elasticsearch HTTP service.
        kib_host : str
            Host running Kibana instance connected to Elasticsearch cluster.
        kib_port : int
            Port number running Elasticsearch HTTP service.
        user : str
            User ID to label Elasticsearch indices and Kibana objects.
        project : str
            Project ID to label Elasticsearch indices and Kibana objects.
        populate_template : bool
            Whether to send documents to indices with the [template] prefix,
            in addition to sending them to the [user_run] prefix. Defaults to
            false.
        modules : list
            List of modules to include from the Kibana templates. Defaults to
            including only the core templates.
    """
    _mutable = {}

    def __init__(self, es_host, es_port, kib_host, kib_port, project,
                 modules=None, populate_template=False):
        self.es_host = es_host
        self.es_port = es_port
        self.kib_host = kib_host
        self.kib_port = kib_port
        self.user = os.environ['USER']
        self.project = project
        self.populate_template = populate_template
        self.modules = modules or ['core']
        self.prefix = '[' + self.user + '_' + self.project + ']'
        self.start = None
        self.client = es.Elasticsearch([{'host': self.es_host,
                                         'port': self.es_port}])

    def __getstate__(self):
        state = {'es_host': self.es_host,
                 'es_port': self.es_port,
                 'kib_host': self.kib_host,
                 'kib_port': self.kib_port,
                 'user': self.user,
                 'project': self.project,
                 'populate_template': self.populate_template,
                 'modules': self.modules,
                 'prefix': self.prefix,
                 'start': self.start}
        return state

    def __setstate__(self, state):
        self.es_host = state['es_host']
        self.es_port = state['es_port']
        self.kib_host = state['kib_host']
        self.kib_port = state['kib_port']
        self.user = state['user']
        self.project = state['project']
        self.modules = state['modules']
        self.populate_template = state['populate_template']
        self.prefix = state['prefix']
        self.start = state['start']
        self.client = es.Elasticsearch([{'host': self.es_host,
                                         'port': self.es_port}])

    def create(self):
        logger.info("checking Elasticsearch indices")

        try:
            indices = self.client.indices.get_aliases().keys()
        except es.exceptions.ConnectionError as e:
            logger.error(e)
            raise e

        if any(self.prefix in s for s in indices):
            logger.info("Elasticsearch indices with prefix " + self.prefix +
                        " already exist")
            self.delete_elasticsearch_indices()

        self.start = datetime.utcnow()

        self.generate_kibana_objects()

    def generate_kibana_objects(self):
        logger.info("generating Kibana objects from templates")

        any_prefix = re.compile('\[.*\]')

        search_index = es_dsl.Search(using=self.client, index='.kibana') \
            .filter('prefix', _id='[template]') \
            .filter('match', _type='index-pattern')
        response_index = search_index.execute()

        for index in response_index:
            index.meta.id = index.meta.id.replace('[template]', self.prefix)
            index.title = index.title.replace('[template]', self.prefix)
            self.client.index(index='.kibana', doc_type=index.meta.doc_type,
                              id=index.meta.id, body=index.to_dict())

        dash_links = {}

        for module in self.modules:
            search_vis = es_dsl.Search(using=self.client, index='.kibana') \
                .filter('prefix', _id='[template][' + module + ']') \
                .filter('match', _type='visualization')

            search_dash = es_dsl.Search(using=self.client, index='.kibana') \
                .filter('prefix', _id='[template][' + module + ']') \
                .filter('match', _type='dashboard')

            response_vis = search_vis.execute()
            response_dash = search_dash.execute()

            for vis in response_vis:
                vis.meta.id = vis.meta.id.replace('[template]', self.prefix)
                vis.title = vis.title.replace('[template]', self.prefix)

                source = json.loads(vis.kibanaSavedObjectMeta.searchSourceJSON)
                source['index'] = any_prefix.sub(self.prefix, source['index'])
                vis.kibanaSavedObjectMeta.searchSourceJSON = json.dumps(source)

                self.client.index(index='.kibana', doc_type=vis.meta.doc_type,
                                  id=vis.meta.id, body=vis.to_dict())

            for dash in response_dash:
                dash.meta.id = dash.meta.id.replace('[template]', self.prefix)
                dash.title = dash.title.replace('[template]', self.prefix)

                dash_panels = json.loads(dash.panelsJSON)
                for panel in dash_panels:
                    panel['id'] = panel['id'].replace(
                        '[template]', self.prefix)
                dash.panelsJSON = json.dumps(dash_panels)

                self.client.index(index='.kibana', doc_type=dash.meta.doc_type,
                                  id=dash.meta.id, body=dash.to_dict())

                link = requests.utils.quote(
                    "http://" + self.kib_host + ":" + str(self.kib_port) +
                    "/app/kibana#/dashboard/" + dash.meta.id +
                    "?_g=(refreshInterval:(display:'15 minutes',pause:!f," +
                    "section:2,value:900000),time:(from:'" + str(self.start) + "Z',mode:absolute,to:now))", safe='/:!?,=#')

                logger.info("Kibana " + module + " dashboard at " + link)

                dash_links[module] = link

            links_text = "###" + self.user + "'s Dashboards\n"

            for module in dash_links:
                links_text += "- [" + module + "](" + dash_links[module] + \
                    ")\n"

            try:
                links_vis = self.client.get(index='.kibana',
                                            doc_type='visualization',
                                            id='[template]-Links')['_source']

                links_vis['title'] = links_vis[
                    'title'].replace('[template]', self.prefix)

                links_state = json.loads(links_vis['visState'])
                links_state['params']['markdown'] = links_text
                links_vis['visState'] = json.dumps(links_state)

                self.client.index(index='.kibana', doc_type='visualization',
                                  id=self.prefix + "-Links", body=links_vis)
            except es.exceptions.NotFoundError:
                logger.info("template markdown links widget not found")

    def delete_kibana_objects(self):
        logger.info("deleting Kibana objects with prefix " + self.prefix)

        search = elasticsearch_dsl.Search(using=self.client, index='.kibana') \
            .filter('prefix', _id=self.prefix)
        response = search.execute()

        for result in response:
            self.client.delete(index='.kibana', doc_type=result.meta.doc_type,
                               id=result.meta.id)

    def delete_elasticsearch_indices(self):
        logger.info(
            "deleting Elasticsearch indices with prefix " + self.prefix)
        self.client.indices.delete(index=self.prefix + '_*')

    def index_task(self, task):
        logger.debug("parsing task object")

        task = dict([(m, o) for (m, o) in inspect.getmembers(task)
                     if not inspect.isroutine(o) and not m.startswith('__')])

        task.pop('_task')
        task.pop('command')

        task['resources_requested'] = dict(
            [(m, o) for (m, o) in
             inspect.getmembers(task['resources_requested'])
             if not inspect.isroutine(o) and not m.startswith('__')])
        task['resources_allocated'] = dict(
            [(m, o) for (m, o) in
             inspect.getmembers(task['resources_allocated'])
             if not inspect.isroutine(o) and not m.startswith('__')])
        task['resources_measured'] = dict(
            [(m, o) for (m, o) in
             inspect.getmembers(task['resources_measured'])
             if not inspect.isroutine(o) and not m.startswith('__')])

        task['resources_requested'].pop('this')
        task['resources_measured'].pop('this')
        task['resources_allocated'].pop('this')

        task['resources_measured'].pop('peak_times')

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

        task_log = task.pop('output')

        e_p = re.compile(r"Begin Fatal Exception([\s\S]*)End Fatal Exception")
        e_match = e_p.search(task_log)

        if e_match:
            logger.debug("parsing task.log fatal exception")

            task['fatal_exception']['message'] = e_match.group(1)

            e_cat_p = re.compile(r"'(.*)'")
            task['fatal_exception']['exception_category'] = \
                e_cat_p.search(task['fatal_exception']['message']).group(1)

            e_mess_p = re.compile(r"Exception Message:\n(.*)")
            task['fatal_exception']['exception_message'] = \
                e_mess_p.search(task['fatal_exception']['message']).group(1)

        upsert_doc = {'doc': {'Task': task, 'timestamp': task['submit_time']},
                      'doc_as_upsert': True}

        try:
            self.client.update(index=self.prefix + '_lobster_tasks',
                               doc_type='task', id=task['id'],
                               body=upsert_doc)
            if self.populate_template:
                self.client.update(index='[template]_lobster_tasks',
                                   doc_type='task', id=task['id'],
                                   body=upsert_doc)
        except es.exceptions.ConnectionError as e:
            logger.error(e)
        except es.exceptions.TransportError as e:
            logger.error(e)

    def index_task_update(self, task_update):
        logger.debug("parsing task update")

        task_update = dict(task_update.__dict__)

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

        upsert_doc = {'doc': {'TaskUpdate': task_update},
                      'doc_as_upsert': True}

        try:
            self.client.update(index=self.prefix + '_lobster_tasks',
                               doc_type='task', id=task_update['id'],
                               body=upsert_doc)
            if self.populate_template:
                self.client.update(index='[template]_lobster_tasks',
                                   doc_type='task', id=task_update['id'],
                                   body=upsert_doc)
        except es.exceptions.ConnectionError as e:
            logger.error(e)
        except es.exceptions.TransportError as e:
            logger.error(e)

    def index_stats(self, now, left, times, log_attributes, stats, category):
        logger.debug("parsing lobster stats log")

        keys = ['timestamp', 'units_left'] + \
            ['total_{}_time'.format(k) for k in sorted(times.keys())] + \
            log_attributes + ['category']

        values = [datetime.utcfromtimestamp(int(now.strftime('%s'))), left] + \
            [times[k] for k in sorted(times.keys())] + \
            [getattr(stats, a) for a in log_attributes] + [category]

        stats = dict(zip(keys, values))

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
        except es.exceptions.ConnectionError as e:
            logger.error(e)
        except es.exceptions.TransportError as e:
            logger.error(e)
