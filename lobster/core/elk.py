import elasticsearch as es
import elasticsearch_dsl as es_dsl
import re
import json

from lobster.util import Configurable


class ElkInterface(Configurable):
    """
    Enables ELK stack monitoring using an existing Elasticsearch instance
    for the current Lobster run.

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
        self.client = es.Elasticsearch([{'host': self.host,
                                         'port': self.port}])
        self.generate_kibana_objects()

    def __getstate__(self):
        state = {'host': self.host,
                 'port': self.port,
                 'user': self.user,
                 'project': self.project,
                 'modules': self.modules}
        return state

    def __setstate__(self, state):
        self.host = state['host']
        self.port = state['port']
        self.user = state['user']
        self.project = state['project']
        self.modules = state['modules']
        self.client = es.Elasticsearch([{'host': self.host,
                                         'port': self.port}])

    def generate_kibana_objects(self):
        temp_prefix = '[template]'
        new_prefix = '[' + self.user + '_' + self.project + ']'
        other_prefix = re.compile('\[.*\]')

        search_index = es_dsl.Search(using=self.client, index='.kibana') \
            .filter('prefix', _id=temp_prefix) \
            .filter('match', _type='index-pattern')
        response_index = search_index.execute()

        for index in response_index:
            index.meta.id = index.meta.id.replace(temp_prefix, new_prefix, 1)
            index.title = index.title.replace(temp_prefix, new_prefix, 1)
            self.client.index(index='.kibana', doc_type=index.meta.doc_type,
                              id=index.meta.id, body=index.to_dict())

        for module in self.modules:
            temp_mod_prefix = temp_prefix + '[' + module + ']'
            new_mod_prefix = new_prefix + '[' + module + ']'

            search_vis = es_dsl.Search(using=self.client, index='.kibana') \
                .filter('prefix', _id=temp_mod_prefix) \
                .filter('match', _type='visualization')

            search_dash = es_dsl.Search(using=self.client, index='.kibana') \
                .filter('prefix', _id=temp_mod_prefix) \
                .filter('match', _type='dashboard')

            response_vis = search_vis.execute()
            response_dash = search_dash.execute()

            for vis in response_vis:
                vis.meta.id = vis.meta.id.replace(temp_mod_prefix,
                                                  new_mod_prefix, 1)
                vis.title = vis.title.replace(temp_mod_prefix,
                                              new_mod_prefix, 1)

                source = json.loads(vis.kibanaSavedObjectMeta.searchSourceJSON)
                source['index'] = other_prefix.sub(new_prefix, source['index'])
                vis.kibanaSavedObjectMeta.searchSourceJSON = json.dumps(source)

                self.client.index(index='.kibana', doc_type=vis.meta.doc_type,
                                  id=vis.meta.id, body=vis.to_dict())

            for dash in response_dash:
                dash.meta.id = dash.meta.id.replace(temp_mod_prefix,
                                                    new_mod_prefix, 1)
                dash.title = dash.title.replace(temp_mod_prefix,
                                                new_mod_prefix, 1)

                dash_panels = json.loads(dash.panelsJSON)
                for panel in dash_panels:
                    panel['id'] = panel['id'].replace(temp_mod_prefix,
                                                      new_mod_prefix, 1)
                dash.panelsJSON = json.dumps(dash_panels)

                self.client.index(index='.kibana', doc_type=dash.meta.doc_type,
                                  id=dash.meta.id, body=dash.to_dict())
