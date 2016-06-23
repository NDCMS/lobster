import elasticsearch as es
import elasticsearch_dsl as es_dsl
import re
import json
import inspect
from datetime import datetime

from lobster.util import Configurable


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
        indices = self.client.indices.get_aliases().keys()
        if any(self.prefix in s for s in indices):
            raise AttributeError("Elasticsearch indices with prefix " +
                                 self.prefix + " already exist.")

        search = es_dsl.Search(using=self.client, index='.kibana') \
            .filter('prefix', _id=self.prefix)
        response = search.execute()
        if len(response) > 0:
            raise AttributeError("Kibana objects with prefix " +
                                 self.prefix + " already exist.")

    def generate_kibana_objects(self):
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
        doc = task.__dict__

        doc['runtime'] = \
            doc['time_processing_end'] - doc['time_wrapper_start']
        doc['time_input_transfer'] = \
            doc['time_transfer_in_start'] - doc['time_transfer_in_end']
        doc['time_startup'] =\
            doc['time_wrapper_start'] - doc['time_transfer_in_end']
        doc['time_release_setup'] = \
            doc['time_wrapper_ready'] - doc['time_wrapper_start']
        doc['time_stage_in'] = \
            doc['time_stage_in_end'] - doc['time_wrapper_ready']
        doc['time_prologue'] = \
            doc['time_prologue_end'] - doc['time_stage_in_end']
        doc['time_overhead'] = \
            doc['time_wrapper_ready'] - doc['time_wrapper_start']
        doc['time_edocecutable'] = \
            doc['time_processing_end'] - doc['time_prologue_end']
        doc['time_epilogue'] = \
            doc['time_epilogue_end'] - doc['time_processing_end']
        doc['time_stage_out'] = \
            doc['time_stage_out_end'] - doc['time_epilogue_end']
        doc['time_output_transfer_wait'] = \
            doc['time_transfer_out_start'] - doc['time_stage_out_end']
        doc['time_output_transfer_work_queue'] = \
            doc['time_transfer_out_end'] - doc['time_transfer_out_start']

        doc['time_processing_end'] = datetime.fromtimestamp(
            doc['time_processing_end'])
        doc['time_prologue_end'] = datetime.fromtimestamp(
            doc['time_prologue_end'])
        doc['time_retrieved'] = datetime.fromtimestamp(
            doc['time_retrieved'])
        doc['time_stage_in_end'] = datetime.fromtimestamp(
            doc['time_stage_in_end'])
        doc['time_stage_out_end'] = datetime.fromtimestamp(
            doc['time_stage_out_end'])
        doc['time_transfer_in_end'] = datetime.fromtimestamp(
            doc['time_transfer_in_end'])
        doc['time_transfer_in_start'] = datetime.fromtimestamp(
            doc['time_transfer_in_start'])
        doc['time_transfer_out_end'] = datetime.fromtimestamp(
            doc['time_transfer_out_end'])
        doc['time_transfer_out_start'] = datetime.fromtimestamp(
            doc['time_transfer_out_start'])
        doc['time_wrapper_ready'] = datetime.fromtimestamp(
            doc['time_wrapper_ready'])
        doc['time_wrapper_start'] = datetime.fromtimestamp(
            doc['time_wrapper_start'])
        doc['time_epilogue_end'] = datetime.fromtimestamp(
            doc['time_epilogue_end'])

        upsert_doc = {'doc': {'lobster_db': doc,
                              'timestamp': doc['time_retrieved']},
                      'doc_as_upsert': True}

        self.client.update(index=self.prefix + '_lobster_tasks',
                           doc_type='task', id=doc['id'], body=upsert_doc)
