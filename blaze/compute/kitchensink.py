from __future__ import absolute_import, division, print_function

try:
    from kitchensink.data import RemoteData, do, du, dp
    from kitchensink import settings
    import kitchensink.serialization.cloudpickle as cloudpickle
except ImportError:
    RemoteData, do, du, dp = (None, None, None, None)

from ..dispatch import dispatch
from ..expr import (Projection)
from .core import compute, compute_up
from ..api.table import Table, Expr

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
                return self.blaze_type(self.remote_data_object.local_path())
            else:
                return self.remote_data_object.obj()

def _discover(blaze_wrapper):
    st = time.time()
    retval = Table(blaze_wrapper.obj()).dshape
    ed = time.time()
    print ('*********', ed - st, ed)
    return retval

import time

@dispatch(SingleBlazeKitchensinkArray)
def discover(blaze_wrapper):
    c = settings.client()
    c.bc(_discover, blaze_wrapper)
    c.execute()
    result = c.br()[0]
    return result

@dispatch(Expr, SingleBlazeKitchensinkArray)
def compute_down(expr, rd, **kwargs):
    c = settings.client()
    c.bc('compute', expr, rd, _rpc_name='blaze')
    c.execute()
    result = c.br()[0]
    return result
