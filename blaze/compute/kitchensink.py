from __future__ import absolute_import, division, print_function

try:
    from kitchensink.data import RemoteData, do, du, dp
    from kitchensink import settings
except ImportError:
    RemoteData, do, du, dp = (None, None, None, None)

from ..dispatch import dispatch
from ..expr import (Projection)
from .core import compute, compute_up
from ..api.table import Table

# class BlazeKitchensinkArray(ChunkIndexable):
#     def __init__(self, seq, **kwargs):
#         super(BlazeKitchensinkArray, self).__init__(seq, **kwargs)
#         new_seq = []
#         for obj in self.seq:
#             if isinstance(obj, SingleBlazeKitchensinkArray
#         self.seq = [

class SingleBlazeKitchensinkArray(object):
    def __init__(self, remote_data_object, blaze_type=None):
        self.blaze_type = None
        self.remote_data_object = remote_data_object
        self._obj = None

    def obj(self):
        if self._obj:
            return self._obj
        else:
            from ..api.table import Table
            if self.blaze_type:
                return Table(self.blaze_type(self.remote_data_object.local_path()))
            else:
                return Table(self.remote_data_object.obj())

def _discover(blaze_wrapper):
    st = time.time()
    table = blaze_wrapper.obj()
    retval =  table.dshape
    ed = time.time()
    print ('*********', ed - st, ed)
    return retval

import time

@dispatch(SingleBlazeKitchensinkArray)
def discover(blaze_wrapper):
    if settings.is_server:
        return _discover(blaze_wrapper)
    else:
        c = settings.client()
        c.bc(_discover, blaze_wrapper)
        c.execute()
        result = c.br()[0]
        return result

def _projection(t, blaze_wrapper, **kwargs):
    return compute(t, blaze_wrapper.obj())

@dispatch(Projection, SingleBlazeKitchensinkArray)
def compute_up(t, blaze_wrapper, **kwargs):
    if settings.is_server:
        return _projection(t, blaze_wrapper, **kwargs)
    else:
        c = settings.client()
        c.bc(_projection, t, blaze_wrapper, **kwargs)
        c.execute()
        result = c.br()[0]
        return result
