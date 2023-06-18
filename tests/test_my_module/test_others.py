import unittest
import os
import base64
from my_module import *


class TestConvertBase64ToIco(unittest.TestCase):

    def test_valid_base64_string(self):
        # This is a simple base64 string, not a real image data
        base64_string = base64.b64encode(b'some data to be converted to .ico file').decode('utf-8')

        try:
            icon_path = convert_base64_to_ico(base64_string)
            self.assertTrue(os.path.exists(icon_path))
            os.remove(icon_path)  # clean up after test
        except Exception as e:
            self.fail(f"Test failed due to {str(e)}")


if __name__ == "__main__":
    unittest.main()
