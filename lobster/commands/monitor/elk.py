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

        logger.debug("checking for task document with ID " + str(task['id']))
        try:
            doc = self.client.get(index=self.prefix + '_lobster_tasks',
                                  doc_type='task', id=task['id'])
            report = doc['report']
            report['task intervals'] = {}

            logger.debug("updating time intervals of document" +
                         str(task['id']))

            report['task intervals']['output transfer wait'] = \
                task['receive_input_start'] - \
                int(report['task timing']['stage out end'].strftime('%s'))
            report['task intervals']['output transfer work queue'] = \
                task['receive_input_finish'] - \
                task['receive_input_start']
            report['task intervals']['input transfer'] = \
                task['send_input_finish'] - \
                task['send_input_start']
            report['task intervals']['startup'] = \
                int(report['task timing']['wrapper start'].strftime('%s')) - \
                task['send_input_start']
        except es.exceptions.NotFoundError:
            pass
        except es.exceptions.ConnectionError as e:
            logger.error(e)

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

    def index_report_json(self, path):
        with open(path, 'r') as f:
            report = json.loads(f.read())

        report.pop('files')

        report['path'] = path

        id_p = re.compile('.*\/0*(\d*)\/0*(\d*)\/.*')
        report['id'] = int(''.join(id_p.search(report['path']).groups()))

        report['task intervals'] = {}

        report['task intervals']['runtime'] = \
            report['task timing']['processing end'] - \
            report['task timing']['wrapper start']
        report['task intervals']['release setup'] = \
            report['task timing']['wrapper ready'] - \
            report['task timing']['wrapper start']
        report['task intervals']['stage in'] = \
            report['task timing']['stage in end'] - \
            report['task timing']['wrapper ready']
        report['task intervals']['prologue'] = \
            report['task timing']['prologue end'] - \
            report['task timing']['stage in end']
        report['task intervals']['overhead'] = \
            report['task timing']['wrapper ready'] - \
            report['task timing']['wrapper start']
        report['task intervals']['executable'] = \
            report['task timing']['processing end'] - \
            report['task timing']['prologue end']
        report['task intervals']['epilogue'] = \
            report['task timing']['epilogue end'] - \
            report['task timing']['processing end']
        report['task intervals']['stage out'] = \
            report['task timing']['stage out end'] - \
            report['task timing']['epilogue end']

        logger.debug("checking for task document with ID " + str(report['id']))
        try:
            doc = self.client.get(index=self.prefix + '_lobster_tasks',
                                  doc_type='task', id=report['id'])
            task = doc['task']

            logger.debug("updating time intervals of document" +
                         str(report['id']))

            report['task intervals']['output transfer wait'] = \
                int(task['receive_input_start'].strftime('%s')) - \
                report['task timing']['stage out end']
            report['task intervals']['output transfer work queue'] = \
                int(task['receive_input_finish'].strftime('%s')) - \
                int(task['receive_input_start'].strftime('%s'))
            report['task intervals']['input transfer'] = \
                int(task['send_input_finish'].strftime('%s')) - \
                int(task['send_input_start'].strftime('%s'))
            report['task intervals']['startup'] = \
                report['task timing']['wrapper start'] - \
                int(task['send_input_start'].strftime('%s'))
        except es.exceptions.NotFoundError:
            pass
        except es.exceptions.ConnectionError as e:
            logger.error(e)

        for field in report['task timing']:
            report['task timing'][field] = datetime.fromtimestamp(
                report['task timing'][field])

        upsert_doc = {'doc': {'report': report},
                      'doc_as_upsert': True}

        logger.debug("sending task document to Elasticsearch")
        try:
            self.client.update(index=self.prefix + '_lobster_tasks',
                               doc_type='task', id=report['id'],
                               body=upsert_doc)
        except es.exceptions.ConnectionError as e:
            logger.error(e)

    def index_work_queue(self, now, left, times, log_attributes, stats):
        logger.debug("parsing work queue log")

        keys = ["timestamp", "units_left"] + \
            ["total_{}_time".format(k) for k in sorted(times.keys())] + \
            log_attributes

        values = [now, left] + \
            [times[k] for k in sorted(times.keys())] + \
            [getattr(stats, a) for a in log_attributes]

        wq = dict(zip(keys, values))

        logger.debug("sending work queue log to Elasticsearch")
        try:
            self.client.index(index=self.prefix + '_work_queue',
                              doc_type='log', body=wq,
                              id=str(int(int(now.strftime('%s')) * 1e6 +
                                         now.microsecond)))
        except es.exceptions.ConnectionError as e:
            logger.error(e)
