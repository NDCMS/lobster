from lobster.core.command import Command


class ElkUpdate(Command):
    @property
    def help(self):
        return 'update Kibana objects'

    def setup(self, argparser):
        pass

    def run(self, args):
        args.config.elk.update_kibana()


class ElkCleanup(Command):
    @property
    def help(self):
        return 'delete Elasticsearch indices and Kibana objects'

    def setup(self, argparser):
        pass

    def run(self, args):
        args.config.elk.cleanup()
