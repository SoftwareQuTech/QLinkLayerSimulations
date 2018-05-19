# Tests of the easy protocol implementation

import unittest
import netsquid.pydynaa as pydynaa

from qlinklayer.localQueue import LocalQueue

class TestLocalQueue(unittest.TestCase):

    def setUp(self):
        pass

    def test_init(self):
        # Test empty case
        standard_queue = LocalQueue()
        assert standard_queue.maxSeq == 2^32 - 1
        assert standard_queue.wsize == standard_queue.maxSeq + 1
        assert standard_queue.scheduleAfter == 0
        assert standard_queue.nextSeq == 0
        assert len(standard_queue.queue) == 0

        # Test setting
        custom_queue = LocalQueue(wsize = 1, maxSeq = 2, scheduleAfter = 3)
        assert custom_queue.wsize == 1
        assert custom_queue.maxSeq == 2
        assert custom_queue.scheduleAfter == 3
        assert custom_queue.nextSeq == 0
        assert len(custom_queue.queue) == 0

    def test_add(self):
        lq = LocalQueue(maxSeq = 5)

        # Next sequence number
        assert lq.nextSeq == 0

        for j in range(5):
            lq.add(0, j+1)
            assert lq.nextSeq == (j+1) % lq.maxSeq


        # Sequence number wraparound
        lq2 = LocalQueue(wsize=10, maxSeq = 20)
        assert lq2.nextSeq == 0

        for j in range(100):
            lq2.add(0, j+1)
            foo = lq2.pop()
            assert lq2.nextSeq == (j+1) % lq2.maxSeq

        # Elements ordered
        lq3 = LocalQueue(maxSeq = 10)
        assert lq2.nextSeq == 0

        for j in range(5):
            lq2.add(0, j+1)
            assert lq2.nextSeq == (j+1) % lq2.maxSeq

        for j in range(5):
            foo = lq2.pop()
            assert foo.seq == j



if __name__ == "__main__":
    unittest.main()
