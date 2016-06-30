from lobster.core.command import Command


class Elk_Update(Command):
    @property
    def help(self):
        return 'update Kibana objects'

    def setup(self, argparser):
        pass

    def run(self, args):
        args.config.elk.update_kibana()


class Elk_Cleanup(Command):
    @property
    def help(self):
        return 'delete Elasticsearch indices and Kibana objects'

    def setup(self, argparser):
        pass

    def run(self, args):
        args.config.elk.cleanup()