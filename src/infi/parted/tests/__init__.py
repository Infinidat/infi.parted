from infi import unittest
from testconfig import config
from .. import Disk

class PartedTestCase(unittest.TestCase):
    @unittest.parameters.iterate("device_path", config.get("devices", []))
    @unittest.parameters.iterate("label_type", ["gpt", "msdos", ])
    def test_partition_table_typw(self, device_path, label_type):
        disk = Disk(device_path)
        disk.create_a_new_partion_table(label_type)
        self.assertEqual(disk.get_partition_table_type(), label_type)
