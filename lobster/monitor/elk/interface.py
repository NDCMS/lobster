import elasticsearch as es
import elasticsearch_dsl as es_dsl
import datetime as dt
import time
import math
import json
import re
import inspect
import logging
import os
import requests

from lobster.util import Configurable, PartiallyMutable
import lobster


logger = logging.getLogger('lobster.monitor.elk')


def dictify(thing, skip=None):
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


def nested_paths(d):
    def get_paths(d, parent=[]):
        if not isinstance(d, dict):
            return [tuple(parent)]
        else:
            return reduce(list.__add__,
                          [get_paths(v, parent + [k])
                           for k, v in d.items()], [])
    return ['.'.join(path) for path in get_paths(d)]


def nested_set(d, path, value):
    keys = path.split('.')
    for key in keys[:-1]:
        d = d.setdefault(key, {})
    d[keys[-1]] = value


def nested_get(d, path):
    keys = path.split('.')
    for key in keys:
        d = d.get(key)
        if d is None:
            break
    return d


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
            self.start_time = dt.datetime.utcnow()
            self.categories = categories
            self.n_categories = len(categories)

            workflows = []
            for category in self.categories:
                workflows += self.categories[category]
            workflows = set(workflows)
            self.n_workflows = len(workflows)

        self.check_client()

        try:
            indices = self.client.indices.get_aliases().keys()
            if any(self.prefix in s for s in indices):
                logger.info("Elasticsearch indices with prefix " +
                            self.prefix + " already exist")
                self.delete_elasticsearch()
        except es.exceptions.ElasticsearchException as e:
            logger.error(e)

        self.init_monitor_data()
        time.sleep(5)

        self.update_kibana()
        logger.info("beginning ELK monitoring")

    def end(self):
        logger.info("ending ELK monitoring")
        with PartiallyMutable.unlock():
            self.end_time = dt.datetime.utcnow()
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
        logger.info("checking Elasticsearch client")
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

    def init_monitor_data(self):
        # FIXME: add a check to see if monitoring data already exists, and
        # if it does, merge the new with the old (only initializing new
        # values), can then run as part of update_kibana() to catch any
        # newly-created histograms or cumulative fields
        logger.info("initializing ELK monitoring data")
        try:
            with open(os.path.join(self.template_dir, 'monitor', 'previous') +
                      '.json', 'r') as f:
                previous = json.load(f)

            for log_type in previous:
                if previous[log_type]['has_categories']:
                    for category in self.categories:
                        previous[log_type][category] = {}
                        for field in nested_paths(
                                previous[log_type]['all']):
                            nested_set(previous[log_type][category],
                                       field, None)

            self.client.index(index=self.prefix + '_monitor_data',
                              doc_type='fields', id='previous',
                              body=previous)
        except Exception as e:
            logger.error(e)

        try:
            with open(os.path.join(self.template_dir, 'monitor', 'intervals') +
                      '.json', 'r') as f:
                intervals = json.load(f)

            vis_id_paths = [path for path in nested_paths(intervals)
                            if path.endswith('.vis_ids')]
            for path in vis_id_paths:
                vis_ids = [vis_id.replace('[template]', self.prefix)
                           for vis_id in nested_get(intervals, path)]
                nested_set(intervals, path, vis_ids)

            self.client.index(index=self.prefix + '_monitor_data',
                              doc_type='fields', id='intervals',
                              body=intervals)
        except Exception as e:
            logger.error(e)

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
        intervals = {}
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
                    .filter('match', _id=self.prefix + '-' + name) \
                    .filter('match', _type='dashboard') \
                    .extra(size=1)
                dash = search_dash.execute()[0]

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
                        .filter('match', _type='visualization') \
                        .extra(size=1)

                    vis = search_vis.execute()[0]
                    vis.meta.id = vis.meta.id \
                        .replace(self.prefix, '[template]')
                    vis.title = vis.title \
                        .replace(self.prefix, '[template]')

                    vis_state = json.loads(vis.visState)
                    vis_state['title'] = vis['title']

                    if vis_state['type'] == 'markdown':
                        vis_state['params']['markdown'] = "text goes here"
                    else:
                        vis_source = json.loads(
                            vis.kibanaSavedObjectMeta.searchSourceJSON)
                        vis_source['index'] = vis_source['index'].replace(
                            self.prefix, '[template]')

                        if vis_state['type'] == 'histogram':
                            hist_aggs = [agg for agg in vis_state['aggs']
                                         if agg['type'] == 'histogram']
                            for agg in hist_aggs:
                                agg['params']['interval'] = 1e10
                                field_path = agg['params']['field']

                                filter_words = \
                                    vis_source['query']['query_string']\
                                    ['query'].split(' ')
                                for i, word in enumerate(filter_words):
                                    if word.startswith(field_path + ':>='):
                                        filter_words[i] = \
                                            field_path + ':>=' + str(0)
                                    elif word.startswith(field_path + ':<='):
                                        filter_words[i] = \
                                            field_path + ':<=' + str(0)
                                vis_source['query']['query_string']['query'] =\
                                    ' '.join(filter_words)
                                # FIXME: add filter if not found to really
                                # make sure we don't accidentally crash with
                                # new templates

                                vis_ids = nested_get(
                                    intervals, field_path + '.vis_ids')

                                if vis_ids and vis.meta.id not in vis_ids:
                                    vis_ids.append(vis.meta.id)
                                else:
                                    vis_ids = [vis.meta.id]

                                hist_data = {
                                    'interval': None,
                                    'min': None,
                                    'max': None,
                                    'vis_ids': vis_ids
                                }

                                nested_set(intervals, agg['params']['field'],
                                           hist_data)
                        elif vis_state['type'] == 'table':
                            if vis.meta.id == '[template]-Category-summary':
                                vis_state['params']['perPage'] = 0
                                aggs = [agg for agg in vis_state['aggs']
                                        if 'params' in agg and
                                        'size' in agg['params']]
                                for agg in aggs:
                                    agg['params']['size'] = 0
                            elif vis.meta.id == '[template]-Workflow-summary':
                                vis_state['params']['perPage'] = 0
                                aggs = [agg for agg in vis_state['aggs']
                                        if 'params' in agg and
                                        'size' in agg['params']]
                                for agg in aggs:
                                    agg['params']['size'] = 0

                        vis.kibanaSavedObjectMeta.searchSourceJSON = \
                            json.dumps(vis_source, sort_keys=True)

                    vis.visState = json.dumps(vis_state, sort_keys=True)

                    with open(os.path.join(vis_dir, vis.meta.id) +
                              '.json', 'w') as f:
                        f.write(json.dumps(vis.to_dict(), indent=4,
                                           sort_keys=True))
                        f.write('\n')
                except Exception as e:
                    logger.error(e)

        try:
            with open(os.path.join(self.template_dir, 'monitor', 'intervals') +
                      '.json', 'w') as f:
                f.write(json.dumps(intervals, indent=4, sort_keys=True))
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
                    vis_id = \
                        vis_path.replace('[template]', self.prefix)[:-5]

                    vis_state = json.loads(vis['visState'])

                    if not vis_state['type'] == 'markdown':
                        source = json.loads(
                            vis['kibanaSavedObjectMeta']['searchSourceJSON'])

                        source['index'] = source['index'] \
                            .replace('[template]', self.prefix)
                        vis['kibanaSavedObjectMeta']['searchSourceJSON'] = \
                            json.dumps(source, sort_keys=True)

                        if vis_state['type'] == 'table':
                            if vis_id == '[template]-Category-summary':
                                vis_state['params']['perPage'] = \
                                    self.n_categories
                                aggs = [agg for agg in vis_state['aggs']
                                        if 'params' in agg and
                                        'size' in agg['params']]
                                for agg in aggs:
                                    agg['params']['size'] = self.n_categories
                            elif vis_id == '[template]-Workflow-summary':
                                vis_state['params']['perPage'] = \
                                    self.n_workflows
                                aggs = [agg for agg in vis_state['aggs']
                                        if 'params' in agg and
                                        'size' in agg['params']]
                                for agg in aggs:
                                    agg['params']['size'] = self.n_workflows

                    vis['visState'] = json.dumps(vis_state)

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
                    "'_missing_:category OR category:all')))",
                    safe='/:!?,&=#')
                links_text += "- [all]({0}?{1})\n" \
                    .format(dash_links[name], all_filter)

                merge_filter = requests.utils.quote(
                    "&_a=(query:(query_string:(analyze_wildcard:!t,query:" +
                    "'_missing_:Task.category OR Task.category:merge')))",
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

    def unroll_cumulative_fields(self, log, log_type, category=None):
        # maybe this shouldn't use its own document? maybe this should instead
        # just fetch the most recent log of its type. would probably be more
        # versatile - would just pass the list of fields that need to be
        # unrolled and any special search parameters to narrow down the log,
        # or do the search in the index function and just pass the old log,
        # the new log, and the list of fields. would also no longer require a
        # json list of cumulative fields in lobster/monitor/elk/data/monitor
        logger.debug("unrolling " + log_type + " cumulative fields")
        try:
            search = es_dsl.Search(
                using=self.client, index=self.prefix + '_monitor_data') \
                .filter('match', _type='fields') \
                .filter('match', _id='previous') \
                .extra(size=1)

            previous = search.execute()[0].to_dict()

            if category is None:
                previous_subset = previous[log_type]
            else:
                previous_subset = previous[log_type][category]

            paths = [path for path in nested_paths(previous_subset)]

            for path in paths:
                cur_val = nested_get(log, path)

                if isinstance(cur_val, dt.date):
                    cur_val = int(cur_val.strftime('%s'))

                old_val = nested_get(previous_subset, path)

                if old_val is not None:
                    if '.' in path:
                        path_parts = path.split('.')
                        new_path = '.'.join(path_parts[:-1] +
                                            [path_parts[-1] + '_diff'])
                    else:
                        new_path = path + '_diff'
                    new_val = max(cur_val - old_val, 0)
                    nested_set(log, new_path, new_val)

                nested_set(previous_subset, path, cur_val)

                if category is None:
                    previous[log_type] = previous_subset
                else:
                    previous[log_type][category] = previous_subset

                self.client.index(index=self.prefix + '_monitor_data',
                                  doc_type='fields', id='previous',
                                  body=previous)
        except Exception as e:
            logger.error(e)

        return log

    def update_histogram_bins(self, log, log_type):
        logger.debug("updating " + log_type + " histogram bins")

        try:
            search = es_dsl.Search(
                using=self.client, index=self.prefix + '_monitor_data') \
                .filter('match', _type='fields') \
                .filter('match', _id='intervals') \
                .extra(size=1)

            intervals = search.execute()[0].to_dict()

            fields = ['.'.join(path.split('.')[:-1])
                      for path in nested_paths(intervals[log_type])
                      if path.endswith('interval')]

            for field in fields:
                cur_val = nested_get(log, field)
                if cur_val is None:
                    break

                field_path = log_type + '.' + field
                intervals_field = nested_get(intervals, field_path)

                changed = False
                if intervals_field['interval'] is None:
                    intervals_field['min'] = cur_val
                    intervals_field['max'] = cur_val
                    changed = True
                else:
                    if cur_val < intervals_field['min']:
                        intervals_field['min'] = cur_val
                        changed = True
                    elif cur_val > intervals_field['max']:
                        intervals_field['max'] = cur_val
                        changed = True

                if changed:
                    if intervals_field['min'] == intervals_field['max']:
                        intervals_field['interval'] = 1
                    else:
                        intervals_field['interval'] = \
                            math.ceil((intervals_field['max'] -
                                       intervals_field['min']) / 20.0)

                    for vis_id in intervals_field['vis_ids']:
                        search_vis = es_dsl.Search(
                            using=self.client, index='.kibana') \
                            .filter('match', _id=vis_id) \
                            .filter('match', _type='visualization') \
                            .extra(size=1)

                        vis = search_vis.execute()[0]
                        vis_state = json.loads(vis.visState)

                        for agg in vis_state['aggs']:
                            if agg['type'] == 'histogram' and \
                                    agg['params']['field'] == field_path:
                                agg['params']['interval'] = \
                                    intervals_field['interval']

                        vis.visState = json.dumps(vis_state, sort_keys=True)

                        vis_source = json.loads(
                            vis.kibanaSavedObjectMeta.searchSourceJSON)

                        filter_words = \
                            vis_source['query']['query_string']\
                            ['query'].split(' ')

                        for i, word in enumerate(filter_words):
                            if word.startswith(field_path + ':>='):
                                filter_words[i] = field_path + ':>=' + \
                                    str(intervals_field['min'])
                            elif word.startswith(field_path + ':<='):
                                filter_words[i] = field_path + ':<=' + \
                                    str(intervals_field['max'])

                        vis_source['query']['query_string']['query'] =\
                            ' '.join(filter_words)

                        vis.kibanaSavedObjectMeta.searchSourceJSON = \
                            json.dumps(vis_source, sort_keys=True)

                        self.client.index(index='.kibana',
                                          doc_type='visualization',
                                          id=vis_id, body=vis.to_dict())

                nested_set(intervals, log_type + '.' + field, intervals_field)

            self.client.index(index=self.prefix + '_monitor_data',
                              doc_type='fields', id='intervals',
                              body=intervals)
        except Exception as e:
            logger.error(e)

    def index_task(self, task):
        logger.debug("parsing Task object")
        try:
            task = dictify(task, skip=('_task'))

            task['resources_requested'] = dictify(
                task['resources_requested'], skip=('this'))
            task['resources_measured'] = dictify(
                task['resources_measured'], skip=('this', 'peak_times'))
            task['resources_allocated'] = dictify(
                task['resources_allocated'], skip=('this'))

            task['resources_measured']['cpu_wall_ratio'] = \
                task['resources_measured']['cpu_time'] / \
                float(task['resources_measured']['wall_time'])
        except Exception as e:
            logger.error(e)
            return

        logger.debug("parsing Task timestamps")
        try:
            timestamp_keys = ['send_input_start',
                              'send_input_finish',
                              'execute_cmd_start',
                              'execute_cmd_finish',
                              'receive_output_start',
                              'receive_output_finish',
                              'submit_time',
                              'finish_time',
                              'resources_measured.start',
                              'resources_measured.end']
            for key in timestamp_keys:
                nested_set(task, key, dt.datetime.utcfromtimestamp(
                    float(str(nested_get(task, key))[:10])))
        except Exception as e:
            logger.error(e)

        logger.debug("parsing Task log")
        try:
            task_log = task.pop('output')
            task['log'] = requests.utils.quote(
                ("kibana#/doc/{0}_lobster_tasks/" +
                 "{0}_lobster_task_logs/log?id={1}")
                .format(self.prefix, task['id']),
                safe='/:!?,&=#')

            e_p = re.compile(
                r"(Begin Fatal Exception[\s\S]*End Fatal Exception)")
            e_match = e_p.search(task_log)

            if e_match:
                logger.debug("parsing fatal exception")

                task['fatal_exception'] = \
                    {'message': e_match.group(1).replace('>> cmd: ', '')}

                e_cat_p = re.compile(r"'(.*)'")
                task['fatal_exception']['category'] = \
                    e_cat_p.search(task['fatal_exception']['message']).group(1)

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

            if not task_update['time_on_worker'] == 0:
                task_update['bandwidth'] = \
                    task_update['network_bytes_received'] / 1e6 / \
                    task_update['time_on_worker']

            if not (task_update['cores'] == 0 or
                    task_update['time_processing_end'] == 0 or
                    task_update['time_prologue_end'] == 0):
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
                max(task_update['time_processing_end'] -
                    task_update['time_wrapper_start'], 0)
            task_update['time_input_transfer'] = \
                max(task_update['time_transfer_in_start'] -
                    task_update['time_transfer_in_end'], 0)
            task_update['time_startup'] = \
                max(task_update['time_wrapper_start'] -
                    task_update['time_transfer_in_end'], 0)
            task_update['time_release_setup'] = \
                max(task_update['time_wrapper_ready'] -
                    task_update['time_wrapper_start'], 0)
            task_update['time_stage_in'] = \
                max(task_update['time_stage_in_end'] -
                    task_update['time_wrapper_ready'], 0)
            task_update['time_prologue'] = \
                max(task_update['time_prologue_end'] -
                    task_update['time_stage_in_end'], 0)
            task_update['time_overhead'] = \
                max(task_update['time_wrapper_ready'] -
                    task_update['time_wrapper_start'], 0)
            task_update['time_executable'] = \
                max(task_update['time_processing_end'] -
                    task_update['time_prologue_end'], 0)
            task_update['time_epilogue'] = \
                max(task_update['time_epilogue_end'] -
                    task_update['time_processing_end'], 0)
            task_update['time_stage_out'] = \
                max(task_update['time_stage_out_end'] -
                    task_update['time_epilogue_end'], 0)
            task_update['time_output_transfer_wait'] = \
                max(task_update['time_transfer_out_start'] -
                    task_update['time_stage_out_end'], 0)
            task_update['time_output_transfer_work_queue'] = \
                max(task_update['time_transfer_out_end'] -
                    task_update['time_transfer_out_start'], 0)
            task_update['time_total_eviction'] = \
                max(task_update['time_total_on_worker'] -
                    task_update['time_on_worker'], 0)

            if task_update['exit_code'] == 0:
                task_update['time_total_overhead'] = \
                    max(task_update['time_prologue_end'] -
                        task_update['time_transfer_in_start'], 0)
                task_update['time_total_processing'] = \
                    max(task_update['time_processing_end'] -
                        task_update['time_prologue_end'], 0)
                task_update['time_total_stage_out'] = \
                    max(task_update['time_transfer_out_end'] -
                        task_update['time_processing_end'], 0)
            else:
                task_update['time_total_failed'] = \
                    task_update['time_total_on_worker']
        except Exception as e:
            logger.error(e)

        logger.debug("parsing TaskUpdate timestamps")
        try:
            timestamp_keys = ['time_processing_end',
                              'time_prologue_end',
                              'time_retrieved',
                              'time_stage_in_end',
                              'time_stage_out_end',
                              'time_transfer_in_end',
                              'time_transfer_in_start',
                              'time_transfer_out_end',
                              'time_transfer_out_start',
                              'time_wrapper_ready',
                              'time_wrapper_start',
                              'time_epilogue_end']
            for key in timestamp_keys:
                task_update[key] = dt.datetime.utcfromtimestamp(
                    float(str(task_update[key])[:10]))
        except Exception as e:
            logger.error(e)

        self.update_histogram_bins(task_update, 'TaskUpdate')

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
                [dt.datetime.utcfromtimestamp(int(now.strftime('%s'))), left] + \
                [times[k] for k in sorted(times.keys())] + \
                [getattr(stats, a) for a in log_attributes] + [category]

            stats = dict(zip(keys, values))

            stats['committed_memory_GB'] = stats['committed_memory'] / 1024.0
            stats['total_memory_GB'] = stats['total_memory'] / 1024.0

            stats['committed_disk_GB'] = stats['committed_disk'] / 1024.0
            stats['total_disk_GB'] = stats['total_disk'] / 1024.0

            stats['start_time'] = dt.datetime.utcfromtimestamp(
                float(str(stats['start_time'])[:10]))
            stats['time_when_started'] = dt.datetime.utcfromtimestamp(
                float(str(stats['time_when_started'])[:10]))
        except Exception as e:
            logger.error(e)
            return

        stats = self.unroll_cumulative_fields(stats, 'stats', category)

        try:
            if 'timestamp_diff' in stats:
                stats['time_other_lobster'] = \
                    max(stats['timestamp_diff'] -
                        stats['total_status_time'] -
                        stats['total_create_time'] -
                        stats['total_action_time'] -
                        stats['total_update_time'] -
                        stats['total_fetch_time'] -
                        stats['total_return_time'], 0)

                stats['time_other_wq'] = \
                    max(stats['timestamp_diff'] - stats['time_send'] -
                        stats['time_receive'] - stats['time_status_msgs'] -
                        stats['time_internal'] - stats['time_polling'] -
                        stats['time_application'], 0)

                stats['time_idle'] = \
                    stats['timestamp_diff'] * stats['idle_percentage']
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

    def index_summary(self, summary):
        logger.debug("updating summary documents")

        keys = ['label', 'events', 'events_read', 'events_written', 'units',
                'units_unmasked', 'units_written', 'units_merged',
                'units_paused', 'units_failed', 'units_skipped',
                'percent_progress', 'percent_merged']

        workflow_summaries = {}

        for item in list(summary)[1:]:
            workflow_summary = dict(zip(keys, item))
            workflow_summary['percent_progress'] = \
                float(workflow_summary['percent_progress'].replace('%', ''))
            workflow_summary['percent_merged'] = \
                float(workflow_summary['percent_merged'].replace('%', ''))

            workflow_summaries[workflow_summary['label']] = workflow_summary

            workflow_doc = {'doc': workflow_summary,
                            'doc_as_upsert': True}

            if workflow_summary['label'] == 'Total':
                self.client.update(
                    index=self.prefix + '_lobster_summaries',
                    doc_type='total', body=workflow_doc,
                    id=workflow_summary['label'])
            else:
                self.client.update(
                    index=self.prefix + '_lobster_summaries',
                    doc_type='workflow', body=workflow_doc,
                    id=workflow_summary['label'])

        for category in self.categories:
            category_summary = {key: 0 for key in keys[1:-2]}

            for workflow in self.categories[category]:
                for item in category_summary:
                    category_summary[item] += \
                        workflow_summaries[workflow][item]

            category_summary['label'] = category
            category_summary['percent_progress'] = round(
                100.0 * category_summary['units_written'] /
                category_summary['units_unmasked'], 1)
            category_summary['percent_merged'] = round(
                100.0 * category_summary['units_merged'] /
                category_summary['units_written'], 1) \
                if category_summary['units_written'] > 0 else 0

            category_doc = {'doc': category_summary,
                            'doc_as_upsert': True}

            self.client.update(
                index=self.prefix + '_lobster_summaries',
                doc_type='category', body=category_doc,
                id=category)
