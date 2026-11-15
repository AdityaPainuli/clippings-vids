
import unittest
from unittest.mock import patch

from localapp.launcher import find_available_port


class TestFindAvailablePort(unittest.TestCase):

    @patch("localapp.launcher.socket.socket")
    def test_tries_next_port_when_port_is_occupied(self, mock_socket):
        mock_sock = mock_socket.return_value.__enter__.return_value

        mock_sock.bind.side_effect = [
            OSError("Port already in use"),
            None,
        ]

        result = find_available_port("127.0.0.1", 8756)

        self.assertEqual(result, 8757)


if __name__ == "__main__":
    unittest.main()
