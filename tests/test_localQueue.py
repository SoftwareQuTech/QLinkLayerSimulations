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
        self.assertEqual(standard_queue.maxSeq, 2 ** 8)
        self.assertEqual(len(standard_queue.queue), 0)
        self.assertEqual(len(standard_queue.sequence_to_item), 0)

        # Test setting
        custom_queue = LocalQueue(maxSeq=2)
        self.assertEqual(custom_queue.maxSeq, 2)
        self.assertEqual(len(custom_queue.queue), 0)
        self.assertEqual(len(custom_queue.sequence_to_item), 0)

    def test_add(self):
        lq = LocalQueue(maxSeq=5)

        for j in range(5):
            req = j + 1
            lq.add(0, req)
            self.assertEqual(lq.queue[-1].request, req)
        self.assertEqual(len(lq.queue), 5)
        self.assertEqual(len(lq.sequence_to_item), 5)
        for seq, item in lq.sequence_to_item.items():
            self.assertTrue(item in lq.queue)

        # Sequence number wraparound
        lq2 = LocalQueue(maxSeq=20)

        for j in range(100):
            req = j + 1
            seq = lq2.add(0, req)
            lq2.ack(seq)
            lq2.ready(seq)
            self.assertEqual(lq2.queue[-1].request, req)
            self.assertEqual(lq2.sequence_to_item[seq].request, req)
            lq2.pop()
        self.assertEqual(len(lq2.queue), 0)
        self.assertEqual(len(lq2.sequence_to_item), 0)

        # Elements ordered
        lq3 = LocalQueue(maxSeq=10)

        for j in range(5):
            seq = lq3.add(0, j + 1)
            lq3.ack(seq)
            lq3.ready(seq)

        for j in range(5):
            foo = lq3.pop()
            assert foo.seq == j

    def test_ordering_for_wrap_around(self):
        quarter = 25
        lq = LocalQueue(maxSeq=4 * quarter)

        # Add 3/4 of the max number
        for i in range(3 * quarter):
            req = i
            seq = lq.add(0, req)
            lq.ack(seq)
            lq.ready(seq)

        # Remove 1/4
        for _ in range(quarter):
            lq.pop()

        # Add 1/2
        for i in range(2 * quarter):
            req = i + 3 * quarter
            seq = lq.add(0, req)
            lq.ack(seq)
            lq.ready(seq)

        # Check that queue is full
        with self.assertRaises(LinkLayerException):
            lq.add(0, 0)

        # Check ordering of current requests
        for i in range(4 * quarter):
            req = i + quarter
            self.assertEqual(lq.pop().request, req)

    def test_full_queue(self):
        lq = LocalQueue()

        for j in range(lq.maxSeq):
            lq.add(0, j + 1)

        # Verify that a full queue raises an exception
        with self.assertRaises(LinkLayerException):
            lq.add(0, 0)



if __name__ == "__main__":
    unittest.main()
