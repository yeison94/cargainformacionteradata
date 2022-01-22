import unittest
from ..main import BusinessLogic as BusinessLogic
# from .. import main.main.BusinessLogic
from unittest.mock import MagicMock

class MyTestCase(unittest.TestCase):
    def test_something(self):
        catalogLayoutMock = CatalogLayout()
        catalogLayoutMock.execute = MagicMock(return_value=3)

        # self.assertEqual(True, True)
        self.assertTrue(BusinessLogic.logic_main())


if __name__ == '__main__':
    CatalogLayout.execute('', '')
