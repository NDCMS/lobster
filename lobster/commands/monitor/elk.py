import elasticsearch as es
import elasticsearch_dsl as es_dsl
from datetime import datetime
import json
import re
import inspect
import logging

from lobster.util import Configurable

logger = logging.getLogger('lobster.monitor.elk')


class ElkInterface(Configurable):
    """
    Enables ELK stack monitoring for the current Lobster run using an existing
    Elasticsearch instance.

    Parameters
    ----------
        host : str
            Host running Elasticsearch cluster.
        port : int
            Port number running Elasticsearch HTTP service.
        user : str
            User ID to label Elasticsearch indices and Kibana objects.
        project : str
            Project ID to label Elasticsearch indices and Kibana objects.
        modules : list
            List of modules to include from the Kibana templates. Defaults to
            including only the core templates.
    """
    _mutable = {}

    def __init__(self, host, port, user, project, modules=None):
        self.host = host
        self.port = port
        self.user = user
        self.project = project
        self.modules = modules or ['core']
        self.prefix = '[' + self.user + '_' + self.project + ']'
        self.client = es.Elasticsearch([{'host': self.host,
                                         'port': self.port}])

    def __getstate__(self):
        state = {'host': self.host,
                 'port': self.port,
                 'user': self.user,
                 'project': self.project,
                 'modules': self.modules,
                 'prefix': self.prefix}
        return state

    def __setstate__(self, state):
        self.host = state['host']
        self.port = state['port']
        self.user = state['user']
        self.project = state['project']
        self.modules = state['modules']
        self.prefix = state['prefix']
        self.client = es.Elasticsearch([{'host': self.host,
                                         'port': self.port}])

    def check_prefix(self):
        logger.info("checking Elasticsearch and Kibana prefixes")

        try:
            indices = self.client.indices.get_aliases().keys()
        except es.exceptions.ConnectionError as e:
            logger.error(e)
            raise e

        if any(self.prefix in s for s in indices):
            e = AttributeError("Elasticsearch indices with prefix " +
                               self.prefix + " already exist.")
            logger.error(e)
            raise e

        search = es_dsl.Search(using=self.client, index='.kibana') \
            .filter('prefix', _id=self.prefix)
        response = search.execute()
        if len(response) > 0:
            e = AttributeError("Kibana objects with prefix " +
                               self.prefix + " already exist.")
            logger.error(e)
            raise e

    def generate_kibana_objects(self):
        logger.info("generating Kibana objects from templates")

        temp_prefix = '[template]'
        self.prefix = '[' + self.user + '_' + self.project + ']'
        other_prefix = re.compile('\[.*\]')

        search_index = es_dsl.Search(using=self.client, index='.kibana') \
            .filter('prefix', _id=temp_prefix) \
            .filter('match', _type='index-pattern')
        response_index = search_index.execute()

        for index in response_index:
            index.meta.id = index.meta.id.replace(temp_prefix, self.prefix, 1)
            index.title = index.title.replace(temp_prefix, self.prefix, 1)
            self.client.index(index='.kibana', doc_type=index.meta.doc_type,
                              id=index.meta.id, body=index.to_dict())

        for module in self.modules:
            temp_mod_prefix = temp_prefix + '[' + module + ']'
            new_mod_prefix = self.prefix + '[' + module + ']'

            search_vis = es_dsl.Search(using=self.client, index='.kibana') \
                .filter('prefix', _id=temp_mod_prefix) \
                .filter('match', _type='visualization')

            search_dash = es_dsl.Search(using=self.client, index='.kibana') \
                .filter('prefix', _id=temp_mod_prefix) \
                .filter('match', _type='dashboard')

            response_vis = search_vis.execute()
            response_dash = search_dash.execute()

            for vis in response_vis:
                vis.meta.id = vis.meta.id.replace(
                    temp_mod_prefix, new_mod_prefix, 1)
                vis.title = vis.title.replace(
                    temp_mod_prefix, new_mod_prefix, 1)

                source = json.loads(vis.kibanaSavedObjectMeta.searchSourceJSON)
                source['index'] = other_prefix.sub(
                    self.prefix, source['index'])
                vis.kibanaSavedObjectMeta.searchSourceJSON = json.dumps(source)

                self.client.index(index='.kibana', doc_type=vis.meta.doc_type,
                                  id=vis.meta.id, body=vis.to_dict())

            for dash in response_dash:
                dash.meta.id = dash.meta.id.replace(
                    temp_mod_prefix, new_mod_prefix, 1)
                dash.title = dash.title.replace(
                    temp_mod_prefix, new_mod_prefix, 1)

                dash_panels = json.loads(dash.panelsJSON)
                for panel in dash_panels:
                    panel['id'] = panel['id'].replace(
                        temp_mod_prefix, new_mod_prefix, 1)
                dash.panelsJSON = json.dumps(dash_panels)

                self.client.index(index='.kibana', doc_type=dash.meta.doc_type,
                                  id=dash.meta.id, body=dash.to_dict())

    def index_task(self, task):
        logger.debug("parsing task object")

        task = dict([(m, o) for (m, o) in inspect.getmembers(task)
                     if not inspect.isroutine(o) and not m.startswith('__')])

        task.pop('_task')
        task.pop('command')
        task.pop('output')

        task['resources_requested'] = dict(
            [(m, o) for (m, o) in inspect.getmembers(task['resources_requested'])
             if not inspect.isroutine(o) and not m.startswith('__')])
        task['resources_allocated'] = dict(
            [(m, o) for (m, o) in inspect.getmembers(task['resources_allocated'])
             if not inspect.isroutine(o) and not m.startswith('__')])
        task['resources_measured'] = dict(
            [(m, o) for (m, o) in inspect.getmembers(task['resources_measured'])
             if not inspect.isroutine(o) and not m.startswith('__')])

        task['resources_requested'].pop('this')
        task['resources_measured'].pop('this')
        task['resources_allocated'].pop('this')

        task['resources_measured'].pop('peak_times')
        # task['resources_measured']['peak_times'] = dict(
        #     [(m, o) for (m, o) in
        #      inspect.getmembers(task['resources_measured']['peak_times'])
        #      if not inspect.isroutine(o) and not m.startswith('__')])
        #
        # task['resources_measured']['peak_times'].pop('this')

        task['send_input_start'] = datetime.fromtimestamp(
            float(str(task['send_input_start'])[:10]))
        task['send_input_finish'] = datetime.fromtimestamp(
            float(str(task['send_input_finish'])[:10]))
        task['execute_cmd_start'] = datetime.fromtimestamp(
            float(str(task['execute_cmd_start'])[:10]))
        task['execute_cmd_finish'] = datetime.fromtimestamp(
            float(str(task['execute_cmd_finish'])[:10]))
        task['receive_output_start'] = datetime.fromtimestamp(
            float(str(task['receive_output_start'])[:10]))
        task['receive_output_finish'] = datetime.fromtimestamp(
            float(str(task['receive_output_finish'])[:10]))
        task['submit_time'] = datetime.fromtimestamp(
            float(str(task['submit_time'])[:10]))
        task['finish_time'] = datetime.fromtimestamp(
            float(str(task['finish_time'])[:10]))

        task['resources_measured']['start'] = datetime.fromtimestamp(
            float(str(task['resources_measured']['start'])[:10]))
        task['resources_measured']['end'] = datetime.fromtimestamp(
            float(str(task['resources_measured']['end'])[:10]))

        # task['resources_measured']['peak_times']['start'] = \
        #     datetime.fromtimestamp(float(str(task['resources_measured']
        #                                      ['peak_times']['start'])[:10]))
        # task['resources_measured']['peak_times']['end'] = \
        #     datetime.fromtimestamp(float(str(task['resources_measured']
        #                                      ['peak_times']['end'])[:10]))

        # task_update = task_update.__dict__
        #
        # task_update['runtime'] = \
        #     task_update['time_processing_end'] - \
        #     task_update['time_wrapper_start']
        # task_update['time_input_transfer'] = \
        #     task_update['time_transfer_in_start'] - \
        #     task_update['time_transfer_in_end']
        # task_update['time_startup'] = \
        #     task_update['time_wrapper_start'] - \
        #     task_update['time_transfer_in_end']
        # task_update['time_release_setup'] = \
        #     task_update['time_wrapper_ready'] - \
        #     task_update['time_wrapper_start']
        # task_update['time_stage_in'] = \
        #     task_update['time_stage_in_end'] - \
        #     task_update['time_wrapper_ready']
        # task_update['time_prologue'] = \
        #     task_update['time_prologue_end'] - \
        #     task_update['time_stage_in_end']
        # task_update['time_overhead'] = \
        #     task_update['time_wrapper_ready'] - \
        #     task_update['time_wrapper_start']
        # task_update['time_executable'] = \
        #     task_update['time_processing_end'] - \
        #     task_update['time_prologue_end']
        # task_update['time_epilogue'] = \
        #     task_update['time_epilogue_end'] - \
        #     task_update['time_processing_end']
        # task_update['time_stage_out'] = \
        #     task_update['time_stage_out_end'] - \
        #     task_update['time_epilogue_end']
        # task_update['time_output_transfer_wait'] = \
        #     task_update['time_transfer_out_start'] - \
        #     task_update['time_stage_out_end']
        # task_update['time_output_transfer_work_queue'] = \
        #     task_update['time_transfer_out_end'] - \
        #     task_update['time_transfer_out_start']
        #
        # task_update['time_processing_end'] = datetime.fromtimestamp(
        #     task_update['time_processing_end'])
        # task_update['time_prologue_end'] = datetime.fromtimestamp(
        #     task_update['time_prologue_end'])
        # task_update['time_retrieved'] = datetime.fromtimestamp(
        #     task_update['time_retrieved'])
        # task_update['time_stage_in_end'] = datetime.fromtimestamp(
        #     task_update['time_stage_in_end'])
        # task_update['time_stage_out_end'] = datetime.fromtimestamp(
        #     task_update['time_stage_out_end'])
        # task_update['time_transfer_in_end'] = datetime.fromtimestamp(
        #     task_update['time_transfer_in_end'])
        # task_update['time_transfer_in_start'] = datetime.fromtimestamp(
        #     task_update['time_transfer_in_start'])
        # task_update['time_transfer_out_end'] = datetime.fromtimestamp(
        #     task_update['time_transfer_out_end'])
        # task_update['time_transfer_out_start'] = datetime.fromtimestamp(
        #     task_update['time_transfer_out_start'])
        # task_update['time_wrapper_ready'] = datetime.fromtimestamp(
        #     task_update['time_wrapper_ready'])
        # task_update['time_wrapper_start'] = datetime.fromtimestamp(
        #     task_update['time_wrapper_start'])
        # task_update['time_epilogue_end'] = datetime.fromtimestamp(
        #     task_update['time_epilogue_end'])

        upsert_doc = {'doc': {'task': task,
                              'timestamp': task['submit_time']},
                      'doc_as_upsert': True}

        logger.debug("sending task document to Elasticsearch")
        try:
            self.client.update(index=self.prefix + '_lobster_tasks',
                               doc_type='task', id=task['id'],
                               body=upsert_doc)
        except es.exceptions.ConnectionError as e:
            logger.error(e)

    def index_work_queue(self, log_attributes, times, stats, now):
        pass
