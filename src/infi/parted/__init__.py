__import__("pkg_resources").declare_namespace(__name__)

from infi.exceptools import InfiException, chain
from infi.pyutils.retry import Retryable, WaitAndRetryStrategy, retry_func
from logging import getLogger

log = getLogger(__name__)
START_OFFSET_BY_LABEL_TYPE = dict(gpt=17408, msdos=512)

# pylint: disable=W0710,E1002
# InfiException does inherit from Exception

class PartedException(InfiException):
    pass

class PartedNotInstalledException(PartedException):
    pass

def is_ubuntu():
    from platform import linux_distribution
    return linux_distribution()[0].lower().startswith("ubuntu")

def get_multipath_prefix(disk_access_path):
    # when used with user_friendly_names:
    # redhat: /dev/mapper/mpath[a-z]
    # ubuntu: /dev/mapper/mpath%d+
    # suse: /dev/mapper/mpath[a-z]
    from re import match
    from platform import linux_distribution
    # for redhat / centos 7 - use no prefix
    linux_dist, linux_ver, _id = linux_distribution()
    ldist = linux_dist.lower()
    if (ldist.startswith("red hat") or ldist.startswith("centos")) and linux_ver.split(".")[0] == "7":
        return ''
    if ldist.startswith("ubuntu") and match('.*mpath[0-9]+', disk_access_path):
        return '-part'
    if ldist.startswith("suse"):
        return '_part'
    if match('.*mpath[a-z]+.*', disk_access_path):
        return 'p'
    return '' if any([disk_access_path.endswith(letter) for letter in 'abcdef']) else 'p'

class PartedRuntimeError(PartedException):
    def __init__(self, returncode, error_message):
        super(PartedRuntimeError, self).__init__()
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
    try:
        parted = execute(["parted", "--version", ])
    except OSError:
        raise PartedNotInstalledException()
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
    try:
        return system() == "Linux" and parse_version(_get_parted_version()) >= parse_version("2.0")
    except PartedException:
        return False

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
    try:
        parted = execute(commandline_arguments)
    except OSError:
        raise PartedNotInstalledException()
    parted.wait()
    if parted.get_returncode() != 0:
        log.debug("parted returned non-zero exit code: {}, stderr and stdout to follow".format(parted.get_returncode()))
        log.debug(parted.get_stderr())
        log.debug(parted.get_stdout())
        if "device-mapper: create ioctl" in parted.get_stderr():
            # this happens sometimes on redhat-7
            # at first we added a retry, but the repeating execution printed:
            # You requested a partition from 65536B to 999934464B (sectors 128..1952997).
            # The closest location we can manage is 65024B to 65024B (sectors 127..127).
            # meaning the first execution suceeded to create the partition
            # so now we're just ignore the return code in case we see this message
            return parted.get_stdout()
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

    def get_size_in_bytes(self):
        raise NotImplementedError()

    def get_partitions(self):
        raise NotImplementedError()

class PartedV1(PartedMixin):
    def get_partition_table_type(self):
        # [4]: 'Partition Table: gpt'
        return self.read_partition_table()[4].split(":")[-1].strip()

    def get_size_in_bytes(self):
        # [2]: 'Disk /dev/sdb: 2147MB'
        return int(self.read_partition_table()[2].split(':')[-1].strip()[:-1])

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

    def get_size_in_bytes(self):
        return int(self.read_partition_table()[1].split(':')[1][:-1])

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
    def __init__(self, device_access_path):
        self._device_access_path = device_access_path

    def execute_parted(self, args):
        commandline_arguments = [self._device_access_path]
        commandline_arguments.extend(args)
        return execute_parted(commandline_arguments)

    def read_partition_table(self):
        """:returns: the output of parted --machine <device> print, splitted to lines"""
        return self.execute_parted(["unit", "B", "print"]).splitlines()

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

    def create_a_new_partition_table(self, label_type, alignment_in_bytes=None):
        """:param label_type: one of the following: ['msdos', 'gpt']"""
        # in linux we don't create a reserved partition at the begging on the disk, so there's no alignment here
        assert(label_type in SUPPORTED_DISK_LABELS)
        self.execute_parted(["mklabel", label_type])

    def destroy_partition_table(self):
        # There is no such capability in the parted utility, need to do something else here
        # sugessstion: get the size of the partition table, and write zeroes on top of it
        raise NotImplementedError()

    def _create_gpt_partition(self, name, filesystem_name, start, end):
        args = ["unit", "B", "mkpart", ]
        args.extend([name, filesystem_name, start, end])
        self.execute_parted(args)

    def _create_primary_partition(self, filesystem_name, start, end):
        args = ["unit", "B", "mkpart", ]
        args.extend(["primary", filesystem_name, start, end])
        self.execute_parted(args)

    def create_partition_for_whole_drive(self, filesystem_name, alignment_in_bytes=None):
        if not self.has_partition_table():
            self.create_a_new_partition_table("gpt", alignment_in_bytes)
        label_type = self.get_partition_table_type()
        start = START_OFFSET_BY_LABEL_TYPE.get(label_type)
        if start is None:
            return
        if alignment_in_bytes:
            start_alignment = start % alignment_in_bytes
            if start_alignment:
                start += alignment_in_bytes - start_alignment
        if label_type == "gpt":
            start, end = str(start) + "B", str(self.get_size_in_bytes() - start) + "B"
            self._create_gpt_partition("None", filesystem_name, start, end)
        elif label_type == "msdos":
            start, end = str(start) + "B", str(self.get_size_in_bytes() - start) + "B"
            self._create_primary_partition(filesystem_name, start, end)
        self.force_kernel_to_re_read_partition_table()
        self.wait_for_partition_access_path_to_be_created()

    @retry_func(WaitAndRetryStrategy(max_retries=60, wait=5))
    def wait_for_partition_access_path_to_be_created(self):
        from os import path, readlink
        from glob import glob
        partitions = self.get_partitions()
        if not partitions:
            raise PartedException("Failed to find partition after creating one")
        access_path = partitions[0].get_access_path()
        if not path.exists(access_path):
            log.debug("partitions are {!r}".format([p.get_access_path() for p in partitions]))
            log.debug("globbing /dev/mapper/* returned {!r}".format(glob("/dev/mapper/*")))
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

    @retry_func(WaitAndRetryStrategy(max_retries=120, wait=5))
    def _execute_mkfs(self, filesystem_name, partition_access_path):
        from infi.execute import execute
        log.info("executing mkfs.{} for {}".format(filesystem_name, partition_access_path))
        mkfs = execute(["mkfs.{}".format(filesystem_name), "-F", partition_access_path])
        if mkfs.get_returncode() != 0:
            log.debug("mkfs failed ({}): {} {}".format(mkfs.get_returncode(), mkfs.get_stdout(), mkfs.get_stderr()))
            raise RuntimeError(mkfs.get_stderr())
        log.info("filesystem formatted")

    def _get_partition_acces_path_by_name(self, partition_number):
        prefix = get_multipath_prefix(self._device_access_path) if 'mapper' in self._device_access_path else ''
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


def from_string(capacity_string):
    import capacity
    try:
        return capacity.from_string(capacity_string)
    except ValueError:  # \d+B
        return int(capacity_string[:-1])

class Partition(object):
    def __init__(self, disk_block_access_path, number, start, end, size):
        super(Partition, self).__init__()
        self._disk_block_access_path = disk_block_access_path
        self._number = number
        self._start = start
        self._end = end
        self._size = size

    def get_size_in_bytes(self):
        return min(self._end - self._start, self._size)  # size may be one byte larger

    def execute_parted(self, args):
        commandline_arguments = [self._disk_block_access_path]
        commandline_arguments.extend(args)
        return execute_parted(commandline_arguments)

    def resize(self, size_in_bytes):
        raise NotImplementedError()

    def force_kernel_to_re_read_partition_table(self):
        from infi.execute import execute
        execute(["partprobe", format(self._disk_block_access_path)]).wait()

    def get_filesystem_name(self):
        return self._filesystem or self.get_filesystem_name_from_blkid()

    def get_filesystem_name_from_blkid(self):
        from infi.execute import execute_assert_success
        from re import search
        output = execute_assert_success(["blkid", self.get_access_path()]).get_stdout()
        # HIP-1433 blkid sometimes shows SEC_TYPE
        # https://access.redhat.com/solutions/705653
        # http://ubuntuforums.org/showthread.php?t=1177419
        # For example:
        # UUID="b6e84210-326d-4131-9916-b0fb1d254b5a" SEC_TYPE="ext2" TYPE="ext3"
        return search(r' TYPE="([^\"]+)*"', output).group(1)


class MBRPartition(Partition):
    def __init__(self, disk_block_access_path, number, partition_type, start, end, size, filesystem):
        super(MBRPartition, self).__init__(disk_block_access_path, number, start, end, size)
        self._type = partition_type
        self._filesystem = filesystem

    def get_number(self):
        return int(self._number)

    def get_type(self):
        return self._type

    def get_access_path(self):
        prefix = get_multipath_prefix(self._disk_block_access_path) if 'mapper' in self._disk_block_access_path else ''
        return "{}{}{}".format(self._disk_block_access_path, prefix, self._number)

    def resize(self, size_in_bytes):
        raise NotImplementedError()

    @classmethod
    def from_parted_machine_parsable_line(cls, disk_device_path, line):
        number, start, end, size, filesystem, _type, flags = line.strip(';').split(':')
        return cls(disk_device_path, int(number), _type, from_string(start), from_string(end), from_string(size), filesystem)

    @classmethod
    def from_parted_non_machine_parsable_line(cls, disk_device_path, line, column_indexes):
        column_indexes.append(1024)
        items = [line[column_indexes[index]:column_indexes[index + 1]] for index in range(len(column_indexes) - 1)]
        number, start, end, size, _type, filesystem, flags = [item.strip() for item in items]
        return cls(disk_device_path, int(number), _type, from_string(start), from_string(end), from_string(size), filesystem)


class GUIDPartition(Partition):
    def __init__(self, disk_block_access_path, number, name, start, end, size, filesystem):
        super(GUIDPartition, self).__init__(disk_block_access_path, number, start, end, size)
        self._name = name
        self._filesystem = filesystem

    def get_number(self):
        return int(self._number)

    def get_name(self):
        return self._name

    def get_access_path(self):
        prefix = get_multipath_prefix(self._disk_block_access_path) if 'mapper' in self._disk_block_access_path else ''
        return "{}{}{}".format(self._disk_block_access_path, prefix, self._number)

    @classmethod
    def from_parted_machine_parsable_line(cls, disk_device_path, line):
        number, start, end, size, filesystem, name, flags = line.strip(';').split(':')
        return cls(disk_device_path, int(number), name, from_string(start), from_string(end), from_string(size), filesystem)

    @classmethod
    def from_parted_non_machine_parsable_line(cls, disk_device_path, line, column_indexes):
        column_indexes.append(1024)
        items = [line[column_indexes[index]:column_indexes[index + 1]] for index in range(len(column_indexes) - 1)]
        number, start, end, size, filesystem, name, flags = [item.strip() for item in items]
        return cls(disk_device_path, int(number), name, from_string(start), from_string(end), from_string(size), filesystem)
