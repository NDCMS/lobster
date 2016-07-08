from lobster.core.command import Command


class ElkDownload(Command):
    @property
    def help(self):
        return 'download Kibana objects and save as templates'

    def setup(self, argparser):
        pass

    def run(self, args):
        args.config.elk.download_templates()


class ElkUpdate(Command):
    @property
    def help(self):
        return 'update Kibana objects from templates'

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
