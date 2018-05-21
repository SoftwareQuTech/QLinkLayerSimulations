#
# Local Queue of sorted items to be scheduled
#
# 

import netsquid.pydynaa as pydynaa

from collections import deque

from qlinklayer.general import *



class LocalQueue:
    """
    Local queue of items ordered by sequence number, incl some additional features
    keeping 

    """

    def __init__(self, wsize = None, maxSeq = None, scheduleAfter = 0):

        # Largest possible sequence number before wraparound
        if maxSeq is None:
            self.maxSeq = 2**32
        else:
            self.maxSeq = maxSeq

        # Maximum number of items to hold in the queue at any one time
        if wsize is None:
            self.wsize = self.maxSeq 
        else:
            assert wsize <= self.maxSeq
            self.wsize = wsize

        # Next sequence number to assign
        self.nextSeq = 0

        # Time to first allow execution after addition
        self.scheduleAfter = scheduleAfter

        # Actual queue
        # TODO not a great data structure
        self.queue = {}

        # Current sequence number that can be consumed
        self.popSeq = 0

    def add(self, originID, request):
        """
        Add item to the Queue with a new sequence number, used by master node only.
        """

        # Check how many items are on the queue right now
        l = len(self.queue)
        if l > self.wsize:
            raise LinkLayerException("Local queue full:" + str(l))

        # There is space, create a new queue item
        seq = self.nextSeq
        self.add_with_id(originID, seq, request)

        # Increment the next sequence number to assign
        self.nextSeq = (self.nextSeq + 1) % self.maxSeq

        return seq

    def add_with_id(self, originID, seq, request):
       
        # Compute the minimum time at which this request can be served
        now = pydynaa.DynAASim().current_time
        sa = now + self.scheduleAfter

        # TODO Needs fixing
        lq = _LocalQueueItem(request, seq, sa) 
        self.queue[seq] = lq

    def pop(self):
        """
        Get item off the top of the queue, if it is ready to be scheduled. 
        If no item is available, return None
        """

        if len(self.queue) == 0:
            # No items on queue
            return None

        # Get item off queue
        q = self.queue[self.popSeq]

        # Check if it's ready to be scheduled
        now = pydynaa.DynAASim().current_time

        if q.scheduleAt >= now:
            # Item ready

            # Remove from queue
            self.queue.pop(self.popSeq, None)

            # Increment lower bound of sequence numbers to return next
            self.popSeq = (self.popSeq + 1) % self.maxSeq

            # Return item
            return q

    def contains(self, seq):

        if seq in self.queue:
            return True
        else:
            return False

    def ready(self, seq, minTime):
        """
        Mark the queue item with queue sequence number seq as ready, and mark it to be
        scheduled at least minTime from now.
        """

        try: 
            self.queue[seq].ready = True
        except KeyError:
            # Not in queue, TODO error handling
            return
            

class _LocalQueueItem:


    def __init__(self, request, seq, scheduleAt):

        self.request = request
        self.seq = seq
        self.scheduleAt = scheduleAt

        # Flag whether this queue item is ready to be executed
        self.ready = False








        
    
