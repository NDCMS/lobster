import os

class TaskFileList(list):
    def __str__(self):
        return ' '.join(self)

def insert_id(id, name):
    """Insert task id into filenames

    >>> insert_id(123, "foo.bar")
    'foo_123.bar'
    >>> insert_id(123, "spam")
    'spam_123'
    """
    return "_{0}".format(id).join(os.path.splitext(name))

class Task(object):
    def __init__(self, id, cmd, outputs, inputs=None):
        self._id = id
        self._cmd = cmd
        self._outputs = TaskFileList(outputs)
        self._inputs = TaskFileList(inputs or [])

    def insert_id(self, name):
        """Insert task id into filenames

        >>> t = Task(123, "echo {inputs} > {output[0]}", ['out.txt'])
        >>> t.insert_id("foo.bar")
        'foo_123.bar'
        >>> t.insert_id("spam")
        'spam_123'
        """
        return "_{0}".format(self._id).join(os.path.splitext(name))

    def inputs(self):
        pass

    def outputs(self):
        pass

    def to_makeflow(self):
        """Format task into a Makeflow element

        >>> t = Task(123, "cat {inputs} > {outputs[0]}", ['out_123.txt'], ['foo'])
        >>> t.to_makeflow()
        'out_123.txt: foo\\n\\tcat foo > out_123.txt'
        """
        command = self._cmd.format(
                inputs=self._inputs,
                outputs=self._outputs)

        return "{out}: {inp}\n\t{cmd}".format(
                out=self._outputs,
                inp=self._inputs,
                cmd=command)
