# Tests of the easy protocol implementation

import unittest
from qlinklayer.localQueue import LocalQueue
from qlinklayer.toolbox import LinkLayerException


class TestLocalQueue(unittest.TestCase):

    def setUp(self):
        pass

    def test_init(self):
        # Test empty case
        standard_queue = LocalQueue()
        assert standard_queue.maxSeq == 2 ** 8
        assert standard_queue.wsize == standard_queue.maxSeq
        assert standard_queue.nextSeq == 0
        assert standard_queue.popSeq == 0
        assert len(standard_queue.queue) == 0

        # Test setting
        custom_queue = LocalQueue(wsize=1, maxSeq=2)
        assert custom_queue.wsize == 1
        assert custom_queue.maxSeq == 2
        assert custom_queue.nextSeq == 0
        assert custom_queue.popSeq == 0
        assert len(custom_queue.queue) == 0

    def test_add(self):
        lq = LocalQueue(maxSeq=5)

        # Next sequence number
        assert lq.nextSeq == 0

        for j in range(5):
            lq.add(0, j + 1)
            assert lq.nextSeq == (j + 1) % lq.maxSeq

        # Sequence number wraparound
        lq2 = LocalQueue(wsize=10, maxSeq=20)
        assert lq2.nextSeq == 0

        for j in range(100):
            seq = lq2.nextSeq
            lq2.add(0, j + 1)
            lq2.ready(seq)
            foo = lq2.pop()
            assert lq2.nextSeq == (j + 1) % lq2.maxSeq

        # Elements ordered
        lq3 = LocalQueue(maxSeq=10)
        assert lq3.nextSeq == 0

        for j in range(5):
            seq = lq3.nextSeq
            lq3.add(0, j + 1)
            lq3.ready(seq)
            assert lq3.nextSeq == (j + 1) % lq3.maxSeq

        for j in range(5):
            foo = lq3.pop()
            assert foo.seq == j

    def test_full_queue(self):
        lq = LocalQueue()

        for j in range(lq.maxSeq):
            lq.add(0, j + 1)
            assert lq.nextSeq == (j + 1) % lq.maxSeq

        # Verify that a full queue raises an exception
        with self.assertRaises(LinkLayerException):
            lq.add(0, 0)


if __name__ == "__main__":
    unittest.main()
