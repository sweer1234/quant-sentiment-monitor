from pathlib import Path
import unittest

from app.policy import PolicyError, load_policy


class PolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        root = Path(__file__).resolve().parents[1]
        self.policy = load_policy(root / "policy" / "policy.yaml")

    def test_policy_loaded(self) -> None:
        self.assertGreater(len(self.policy.commands), 5)
        self.assertIn("apt_install", self.policy.commands)

    def test_apt_install_arg_check(self) -> None:
        apt_install = self.policy.get_command("apt_install")
        apt_install.validate_args("apt_install", ["nginx"])
        with self.assertRaises(PolicyError):
            apt_install.validate_args("apt_install", ["nginx;rm"])

    def test_service_status_arg_count(self) -> None:
        service_status = self.policy.get_command("service_status")
        service_status.validate_args("service_status", ["nginx"])
        with self.assertRaises(PolicyError):
            service_status.validate_args("service_status", ["nginx", "mysql"])


if __name__ == "__main__":
    unittest.main()
