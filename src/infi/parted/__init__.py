__import__("pkg_resources").declare_namespace(__name__)

from infi.exceptools import InfiException, chain
from infi.pyutils.retry import Retryable, WaitAndRetryStrategy, retry_method
from logging import getLogger

log = getLogger(__name__)

# pylint: disable=W0710,E1002
# InfiException does inherit from Exception

class PartedException(InfiException):
    pass

def is_ubuntu():
    from platform import dist
    return dist()[0].lower() == "ubuntu"

def get_multipath_prefix():
    return 'p'

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

def _get_parted_version():
    from infi.execute import execute
    parted = execute(["parted", "--version", ])
    parted.wait()
    # stdout possibilities
    # GNU Parted 1.8.1
    # or
    # parted (GNU parted) 2.1
    # Copyright ..
    return parted.get_stdout().splitlines()[0].split()[-1]

def _is_parted_has_machine_parsable_output():
    from pkg_resources import parse_version
    from platform import system
    return system() == "Linux" and parse_version(_get_parted_version()) >= parse_version("2.0")

PARTED_REQUIRED_ARGUMENTS = [
                             "--script", # never prompts for user intervention
                             ]

if _is_parted_has_machine_parsable_output():
    PARTED_REQUIRED_ARGUMENTS.extend(["--machine", ])  # displays machine parseable output

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
    log.debug("executing {}".format(" ".join([repr(item) for item in commandline_arguments])))
    parted = execute(commandline_arguments)
    parted.wait()
    if parted.get_returncode() != 0:
        log.debug("parted returned non-zero exit code: {}".format(parted.get_returncode()))
        if "WARNING" in parted.get_stdout():
            # don't know what's the error code in this case, and failed to re-create it
            return parted.get_stdout()
        if "aligned for best performance" in parted.get_stdout():
            # HIP-330 we something get. according to parted's source, this is a warning
            return parted.get_stdout()
        raise PartedRuntimeError(parted.get_returncode(),
                                 _get_parted_error_message_from_stderr(parted.get_stdout()))
    return parted.get_stdout()


SUPPORTED_DISK_LABELS = ["gpt", "msdos"]

class PartedMixin(object):
    def get_partition_table_type(self):
        """:returns: one of [None, 'gpt' 'msdos', ...]"""
        raise NotImplementedError()

    def get_disk_size(self):
        raise NotImplementedError()

    def get_partitions(self):
        raise NotImplementedError()

class PartedV1(PartedMixin):
    def get_partition_table_type(self):
        # [4]: 'Partition Table: gpt'
        return self.read_partition_table()[4].split(":")[-1].strip()

    def get_disk_size(self):
        # [2]: 'Disk /dev/sdb: 2147MB'
        return self.read_partition_table()[2].split(':')[-1].strip()

    def get_partitions(self):
        if not self.has_partition_table():
            return []
        header = self.read_partition_table()[6]
        if self.get_partition_table_type() == "gpt":
            names = ["Number", "Start", "End", "Size", "File system", "Name", "Flags"]
            column_indexes = [header.index(name) for name in names]
            return [GUIDPartition.from_parted_non_machine_parsable_line(self._device_access_path, line, column_indexes)
                    for line in self.read_partition_table()[7:-1]]
        elif self.get_partition_table_type() == "msdos":
            names = ["Number", "Start", "End", "Size", "Type", "File system", "Flags"]
            column_indexes = [header.index(name) for name in names]
            return [MBRPartition.from_parted_non_machine_parsable_line(self._device_access_path, line, column_indexes)
                    for line in self.read_partition_table()[7:-1]]

class PartedV2(PartedMixin):
    def get_partition_table_type(self):
        return self.read_partition_table()[1].split(':')[5]

    def get_disk_size(self):
        return self.read_partition_table()[1].split(':')[1]

    def get_partitions(self):
        if not self.has_partition_table():
            return []
        if self.get_partition_table_type() == "gpt":
            return [GUIDPartition.from_parted_machine_parsable_line(self._device_access_path, line)
                    for line in self.read_partition_table()[2:]]
        elif self.get_partition_table_type() == "msdos":
            return [MBRPartition.from_parted_machine_parsable_line(self._device_access_path, line)
                    for line in self.read_partition_table()[2:]]

MatchingPartedMixin = PartedV2 if _is_parted_has_machine_parsable_output() else PartedV1

class Disk(MatchingPartedMixin, Retryable, object):
    retry_strategy = WaitAndRetryStrategy(max_retries=30, wait=1)

    def __init__(self, device_access_path):
        self._device_access_path = device_access_path

    def execute_parted(self, args):
        commandline_arguments = [self._device_access_path]
        commandline_arguments.extend(args)
        return execute_parted(commandline_arguments)

    def read_partition_table(self):
        """:returns: the output of parted --machine <device> print, splitted to lines"""
        return self.execute_parted(["print"]).splitlines()

    def has_partition_table(self):
        try:
            self.read_partition_table()
            return True
        except PartedRuntimeError, error:
            if "unrecognised disk label" in error.get_error_message():
                pass
            elif "exceeds the loop-partition-table-impose" in error.get_error_message():
                pass
            else:
                raise chain(InvalidPartitionTable())
        return False

    def create_a_new_partition_table(self, label_type):
        """:param label_type: one of the following: ['msdos', 'gpt']"""
        assert(label_type in SUPPORTED_DISK_LABELS)
        self.execute_parted(["mklabel", label_type])

    def destroy_partition_table(self):
        # There is no such capability in the parted utility, need to do something else here
        # sugessstion: get the size of the partition table, and write zeroes on top of it
        raise NotImplementedError()

    def _create_gpt_partition(self, name, filesystem_name, start, end):
        args = ["mkpart", ]
        args.extend([name, filesystem_name, start, end])
        self.execute_parted(args)

    def _create_primary_partition(self, filesystem_name, start, end):
        args = ["mkpart", ]
        args.extend(["primary", filesystem_name, start, end])
        self.execute_parted(args)

    def create_partition_for_whole_drive(self, filesystem_name):
        if not self.has_partition_table():
            self.create_a_new_partition_table("gpt")
        label_type = self.get_partition_table_type()
        start, end = '0', self.get_disk_size()
        if label_type == "gpt":
            self._create_gpt_partition("None", filesystem_name, start, end)
        elif label_type == "msdos":
            self._create_primary_partition(filesystem_name, start, end)
        self.force_kernel_to_re_read_partition_table()
        self.wait_for_partition_access_path_to_be_created()

    @retry_method
    def wait_for_partition_access_path_to_be_created(self):
        from os import path, readlink
        partitions = self.get_partitions()
        if not partitions:
            raise PartedException("Failed to find partition after creating one")
        access_path = partitions[0].get_access_path()
        if not path.exists(access_path):
            raise PartedException("Block access path for created partition does not exist")
        log.debug("Partition access path {!r} exists".format(access_path))
        if not path.islink(access_path):
            return
        link_path = path.abspath(path.join(path.dirname(access_path), readlink(access_path)))
        if not path.exists(link_path):
            raise PartedException("Read-link Block access path for created partition does not exist")
        log.debug("Read-link Partition access path {!r} exists".format(link_path))

    def force_kernel_to_re_read_partition_table(self):
        from infi.execute import execute
        execute(["partprobe", format(self._device_access_path)]).wait()

    def _execute_mkfs(self, filesystem_name, partition_access_path):
        from infi.execute import execute
        log.info("executing mkfs.{} for {}".format(filesystem_name, partition_access_path))
        mkfs = execute(["mkfs.{}".format(filesystem_name), "-F", partition_access_path])
        if mkfs.get_returncode() != 0:
            raise RuntimeError(mkfs.get_stderr())
        log.info("filesystem formatted")

    def _get_partition_acces_path_by_name(self, partition_number):
        prefix = get_multipath_prefix() if 'mapper' in self._device_access_path else ''
        return "{}{}{}".format(self._device_access_path, prefix, partition_number)

    def format_partition(self, partition_number, filesystem_name, mkfs_options={}): # pylint: disable=W0102
        """currently mkfs_options is ignored"""
        try:
            self.execute_parted(["mkfs", str(partition_number), filesystem_name])
        except PartedRuntimeError, error:
            log.exception("parted error")
            if "not implemented yet" in error.get_error_message():
                pass
            else:
                raise
        self.force_kernel_to_re_read_partition_table()
        partition_access_path = self._get_partition_acces_path_by_name(partition_number)
        self._execute_mkfs(filesystem_name, partition_access_path)

# pylint: disable=R0913

class MBRPartition(object):
    def __init__(self, disk_block_access_path, number, partition_type, size, filesystem):
        super(MBRPartition, self).__init__()
        self._type = partition_type
        self._size = size
        self._number = number
        self._filesystem = filesystem
        self._disk_block_access_path = disk_block_access_path

    def get_number(self):
        return int(self._number)

    def get_type(self):
        return self._type

    def get_size(self):
        return self._size

    def get_access_path(self):
        prefix = get_multipath_prefix() if 'mapper' in self._disk_block_access_path else ''
        return "{}{}{}".format(self._disk_block_access_path, prefix, self._number)

    def get_filesystem_name(self):
        return self._filesystem or None

    @classmethod
    def from_parted_machine_parsable_line(cls, disk_device_path, line):
        from capacity import from_string
        number, start, end, size, filesystem, _type, flags = line.strip(';').split(':')
        return cls(disk_device_path, number, _type, from_string(size), filesystem)

    @classmethod
    def from_parted_non_machine_parsable_line(cls, disk_device_path, line, column_indexes):
        from capacity import from_string
        column_indexes.append(1024)
        items = [line[column_indexes[index]:column_indexes[index + 1]] for index in range(len(column_indexes) - 1)]
        number, start, end, size, _type, filesystem, flags = [item.strip() for item in items]
        return cls(disk_device_path, number, _type, from_string(size), filesystem)

class GUIDPartition(object):
    def __init__(self, disk_block_access_path, number, name, size, filesystem):
        super(GUIDPartition, self).__init__()
        self._name = name
        self._size = size
        self._number = number
        self._filesystem = filesystem
        self._disk_block_access_path = disk_block_access_path

    def get_number(self):
        return int(self._number)

    def get_name(self):
        return self._name

    def get_size(self):
        return self._size

    def get_access_path(self):
        prefix = get_multipath_prefix() if 'mapper' in self._disk_block_access_path else ''
        return "{}{}{}".format(self._disk_block_access_path, prefix, self._number)

    def get_filesystem_name(self):
        return self._filesystem or None

    @classmethod
    def from_parted_machine_parsable_line(cls, disk_device_path, line):
        from capacity import from_string
        number, start, end, size, filesystem, name, flags = line.strip(';').split(':')
        return cls(disk_device_path, number, name, from_string(size), filesystem)

    @classmethod
    def from_parted_non_machine_parsable_line(cls, disk_device_path, line, column_indexes):
        from capacity import from_string
        column_indexes.append(1024)
        items = [line[column_indexes[index]:column_indexes[index + 1]] for index in range(len(column_indexes) - 1)]
        number, start, end, size, filesystem, name, flags = [item.strip() for item in items]
        return cls(disk_device_path, number, name, from_string(size), filesystem)
