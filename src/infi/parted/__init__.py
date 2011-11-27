__import__("pkg_resources").declare_namespace(__name__)

from infi.exceptools import InfiException, chain

class PartedException(InfiException):
    pass

class PartedRuntimeError(PartedException):
    def __init__(self, returncode, error_message):
        super(PartedException, self).__init__()
        self._rc = returncode
        self._em = error_message

    def __str__(self):
        return self._em

    def __repr__(self):
        return "<{}>: {}".format(self._rc, self._em)

    def get_error_message(self):
        return str(self)

class InvalidPartitionTable(PartedException):
    pass

PARTED_REQUIRED_ARGUMENTS = ["--machine", # displays machine parseable output
                             "--script", # never prompts for user intervention
                             ]

def _get_parted_error_message_from_stderr(stderr):
    if stderr.split(':', 1) == []:
        return stderr
    return stderr.split(':', 1)[-1]

def execute_parted(args):
    """This function calls the parted utility and returns its machine parsable output, without user intervention
    :returns: if the call returned success, parted's standard output is returned.
    If the call returned an error, an :class:`PartedException` is raised with the return code and the error mesage"""
    from infi.execute import execute
    commandline_arguments = ["parted", ]
    commandline_arguments.extend(PARTED_REQUIRED_ARGUMENTS)
    commandline_arguments.extend(args)
    parted = execute(commandline_arguments)
    parted.wait()
    if parted.get_returncode() != 0:
        raise PartedRuntimeError(parted.get_returncode(),
                                 _get_parted_error_message_from_stderr(parted.get_stderr()))
    return parted.get_stdout()


SUPPORTED_DISK_LABELS = ["gpt", "msdos"]

class Disk(object):
    def __init__(self, device_access_path):
        self._device_access_path = device_access_path

    def execute_parted(self, args):
        commandline_arguments = [self._device_access_path]
        commandline_arguments.extend(args)
        return execute_parted(commandline_arguments)

    def _read_partition_table(self):
        return self.execute_parted(["print"])

    def has_partition_table(self):
        try:
            self._read_partition_table()
            return True
        except PartedRuntimeError, error:
            if "unrecognised disk label" in error.get_error_message():
                pass
            elif "exceeds the loop-partition-table-impose" in error.get_error_message():
                pass
            else:
                raise chain(InvalidPartitionTable())
        return False

    def create_a_new_partion_table(self, label_type):
        """:param label_type: one of the following: ['msdos', 'gpt']"""
        assert(label_type in SUPPORTED_DISK_LABELS)
        self.execute_parted(["mklabel", label_type])

    def destroy_partition_table(self):
        # There is no such capability in the parted utility, need to do something else here
        # sugessstion: get the size of the partition table, and write zeroes on top of it
        raise NotImplementedError()

    def get_partition_table_type(self):
        """:returns: one of [None, 'gpt' 'msdos', ...]"""
        if not self.has_partition_table():
            return None
        return self._read_partition_table().splitlines()[1].split(':')[5]

    def get_disk_size(self):
        return self._read_partition_table().splitlines()[1].split(':')[1]

    def _create_gpt_partition(self, name, start, end):
        args = ["mkpart", ]
        args.extend([name, start, end])
        self.execute_parted(args)

    def _create_primary_partition(self, start, end):
        args = ["mkpart", ]
        args.extend(["primary", start, end])
        self.execute_parted(args)

    def create_partition_for_whole_drive(self):
        if not self.has_partition_table():
            self.create_a_new_partion_table("gpt")
        label_type = self.get_partition_table_type()
        start, end = '0', self.get_disk_size()
        if label_type == "gpt":
            self._create_gpt_partition("None", start, end)
        elif label_type == "msdos":
            self._create_primary_partition(start, end)
        self.force_kernel_to_re_read_partition_table()

    def get_partitions(self):
        if not self.has_partition_table():
            return []
        return [Partition.from_parted_machine_parsable_line(self._device_access_path, line)
                for line in self._read_partition_table()[2:]]

    def force_kernel_to_re_read_partition_table(self):
        from infi.execute import execute
        execute("partprobe {}".format(self._device_access_path)).wait()

class Partition(object):
    def __init__(self, path, type, size):
        super(Partition, self).__init__()
        self._type = type
        self._size = size
        self.path = path

    def get_number(self):
        return self._number

    def get_type(self):
        return self._type

    def get_size(self):
        return self._size

    def get_access_path(self):
        return "{}{}".format(self._disk, self._number)

    @classmethod
    def from_parted_machine_parsable_line(cls, disk_device_path, line):
        from capacity import from_string
        number, start, end, size, _type, filesystem, flags = line.strip(';').split(':')
        return cls("{}{}".format(disk_device_path, number), _type, from_string(size))
