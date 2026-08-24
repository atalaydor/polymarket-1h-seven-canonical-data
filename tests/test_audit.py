from __future__ import annotations

import json
import unittest

from canonical_data.audit import canonical_json_bytes, offline_audit, sha256_bytes


class AuditTests(unittest.TestCase):
    def test_canonical_json_is_stable(self) -> None:
        left = canonical_json_bytes({"b": 2, "a": [1, "é"]})
        right = canonical_json_bytes(json.loads(left))
        self.assertEqual(left, right)
        expected = "971afaeb462b9680867da4c982521e24d1121a658d4cd0523d1395577025e538"
        self.assertEqual(sha256_bytes(left), expected)

    def test_repository_contract(self) -> None:
        self.assertEqual(offline_audit()["errors"], [])


if __name__ == "__main__":
    unittest.main()
