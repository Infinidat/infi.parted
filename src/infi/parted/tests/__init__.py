from infi import unittest
from testconfig import config
from .. import Disk, MBRPartition, GUIDPartition

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
