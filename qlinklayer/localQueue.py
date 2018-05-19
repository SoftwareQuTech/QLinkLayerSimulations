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

    def __init__(self, wsize = None, maxSeq = (2^32 - 1), scheduleAfter = 0):

        # Largest possible sequence number before wraparound
        self.maxSeq = maxSeq

        # Maximum number of items to hold in the queue at any one time
        if wsize is None:
            self.wsize = maxSeq + 1
        else:
            assert wsize <= maxSeq + 1
            self.wsize = wsize

        # Next sequence number to assign
        self.nextSeq = 0

        # Time to first allow execution after addition
        self.scheduleAfter = scheduleAfter

        # Actual queue
        self.queue = deque()

    def add(self, originID, request):

        # Check how many items are on the queue right now
        l = len(self.queue)
        if l > self.wsize:
            raise LinkLayerException("Local queue full")

        # Compute the minimum time at which this request can be served
        now = pydynaa.DynAASim().current_time
        sa = now + self.scheduleAfter

        # There is space, create a new queue item
        seq = self.nextSeq
        lq = _LocalQueueItem(request, seq, sa)
        self.queue.append(lq)

        # Increment the next sequence number to assign
        self.nextSeq = (self.nextSeq + 1) % self.maxSeq

        return seq

    def add_with_id(self, originID, seq, request):
       
        # Compute the minimum time at which this request can be served
        now = pydynaa.DynAASim().current_time
        sa = now + self.scheduleAfter

        # TODO Needs fixing
        lq = _LocalQueueItem(request, seq, sa) 

    def pop(self):
        """
        Get item off the top of the queue, if it is ready to be scheduled. 
        If no item is available, return None
        """

        if len(self.queue) == 0:
            # No items on queue
            return None

        # Get item off queue
        q = self.queue.popleft()

        # Check if it's ready to be scheduled
        now = pydynaa.DynAASim().current_time

        if q.scheduleAt >= now:
            # Item ready, return it for processing
            return q
        else:
            # Item not yet ready, return None and put it back
            self.queue.appendleft(q)
            return None

    def contains(self, seq):

        # TODO better data structure
        for item in self.queue:
            if item.seq == seq:
                return True

        return False

    def ready(self, seq, minTime):
        """
        Mark the queue item with queue sequence number seq as ready, and mark it to be
        scheduled at least minTime from now.
        """

        # TODO better data structure
        for item in self.queue:
            if item.seq == seq:
                item.ready = True
                return
        

class _LocalQueueItem:


    def __init__(self, request, seq, scheduleAt):

        self.request = request
        self.seq = seq
        self.scheduleAt = scheduleAt

        # Flag whether this queue item is ready to be executed
        self.ready = False








        
    
