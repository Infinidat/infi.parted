from infi import unittest
from testconfig import config
from .. import Disk, MBRPartition, GUIDPartition, get_multipath_prefix

class PartedTestCase(unittest.TestCase):
    @unittest.parameters.iterate("device_path", config.get("devices", []))
    @unittest.parameters.iterate("label_type", ["gpt", "msdos", ])
    def test_partition_table_typw(self, device_path, label_type):
        disk = Disk(device_path)
        disk.create_a_new_partition_table(label_type)
        self.assertEqual(disk.get_partition_table_type(), label_type)

    @unittest.parameters.iterate("device_path", config.get("devices", []))
    @unittest.parameters.iterate("label_type", ["gpt", "msdos", ])
    def test_create_partition_for_whole_drive(self, device_path, label_type):
        disk = Disk(device_path)
        disk.create_a_new_partition_table(label_type)
        self.assertEqual(disk.get_partitions(), [])
        disk.create_partition_for_whole_drive("ext3")
        partitions = disk.get_partitions()
        self.assertEqual(len(partitions), 1)
        self.assertIsInstance(partitions[0], (MBRPartition, GUIDPartition,))
        self.assertIn(partitions[0].get_filesystem_name(), [None, ])
        self.assertEqual(partitions[0].get_number(), 1)

    def test_positive_multipath_prefix(self):
        access_paths_and_prefixes = {
            '/dev/mapper/mpatha-part1': '-part',
            '/dev/mapper/mpathac-part1': '-part',
            '/dev/mapper/mpath21-part1': '-part',
            '/dev/mapper/mpatha-p1': '-p',
            '/dev/mapper/mpath2p1': 'p',
            '/dev/mapper/mpath2-p1': '-p',
            '/dev/mapper/mpath2_part1': '_part'
        }

        for access_path, prefix in access_paths_and_prefixes.iteritems():
            self.assertEqual(get_multipath_prefix(access_path), prefix)

    def test_negative_multipath_prefix(self):
        access_paths = [
            '/dev/mapper/36742b0f000004e2b0000000000000a0e'
            '/dev/mapper/mpath5',
            '/dev/mapper/mpatha',
            '/dev/mapper/mpathaa',
            '/dev/mapper/mpathparta',
            '/dev/mapper/mpathapart',
            '/dev/mapper/mpathpa',
            '/dev/mapper/mpathapa',
            ]

        for access_path in access_paths:
            self.assertEqual(get_multipath_prefix(access_path), '')
