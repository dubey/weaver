import sys

sys.path.append(".libs/")

import chronos

c = chronos.Client('127.0.0.1', 7890)
w = c.create_event()
x = c.create_event()
y = c.create_event()
z = c.create_event()

assert c.query_order([(w, x)]) == [(w, x, '?')]
assert c.query_order([(w, y)]) == [(w, y, '?')]
assert c.query_order([(w, z)]) == [(w, z, '?')]
assert c.query_order([(x, y)]) == [(x, y, '?')]
assert c.query_order([(x, z)]) == [(x, z, '?')]
assert c.query_order([(y, z)]) == [(y, z, '?')]
assert c.query_order([(z, w)]) == [(z, w, '?')]
assert c.query_order([(z, x)]) == [(z, x, '?')]
assert c.query_order([(z, y)]) == [(z, y, '?')]
assert c.query_order([(y, w)]) == [(y, w, '?')]
assert c.query_order([(y, x)]) == [(y, x, '?')]
assert c.query_order([(x, w)]) == [(x, w, '?')]
assert c.query_order(
    [(w,x),    (w,y),    (w,z),    (x,y),    (x,z),    (y,z),    (z,w),    (z,x),    (z,y),    (y,w),    (y,x),    (x,w)]) \
 == [(w,x,'?'),(w,y,'?'),(w,z,'?'),(x,y,'?'),(x,z,'?'),(y,z,'?'),(z,w,'?'),(z,x,'?'),(z,y,'?'),(y,w,'?'),(y,x,'?'),(x,w,'?')]

assert c.assign_order([(w, x, '<')]) == [(w, x, '<')]
assert c.assign_order([(w, x, '>f')]) == [(w, x, '<')]

assert c.query_order([(w, x)]) == [(w, x, '<')]
assert c.query_order([(w, y)]) == [(w, y, '?')]
assert c.query_order([(w, z)]) == [(w, z, '?')]
assert c.query_order([(x, y)]) == [(x, y, '?')]
assert c.query_order([(x, z)]) == [(x, z, '?')]
assert c.query_order([(y, z)]) == [(y, z, '?')]
assert c.query_order([(z, w)]) == [(z, w, '?')]
assert c.query_order([(z, x)]) == [(z, x, '?')]
assert c.query_order([(z, y)]) == [(z, y, '?')]
assert c.query_order([(y, w)]) == [(y, w, '?')]
assert c.query_order([(y, x)]) == [(y, x, '?')]
assert c.query_order([(x, w)]) == [(x, w, '>')]
assert c.query_order(
    [(w,x),    (w,y),    (w,z),    (x,y),    (x,z),    (y,z),    (z,w),    (z,x),    (z,y),    (y,w),    (y,x),    (x,w)]) \
 == [(w,x,'<'),(w,y,'?'),(w,z,'?'),(x,y,'?'),(x,z,'?'),(y,z,'?'),(z,w,'?'),(z,x,'?'),(z,y,'?'),(y,w,'?'),(y,x,'?'),(x,w,'>')]

assert c.assign_order([(y, z, '<')]) == [(y, z, '<')]
assert c.assign_order([(y, z, '>f')]) == [(y, z, '<')]

assert c.query_order([(w, x)]) == [(w, x, '<')]
assert c.query_order([(w, y)]) == [(w, y, '?')]
assert c.query_order([(w, z)]) == [(w, z, '?')]
assert c.query_order([(x, y)]) == [(x, y, '?')]
assert c.query_order([(x, z)]) == [(x, z, '?')]
assert c.query_order([(y, z)]) == [(y, z, '<')]
assert c.query_order([(z, w)]) == [(z, w, '?')]
assert c.query_order([(z, x)]) == [(z, x, '?')]
assert c.query_order([(z, y)]) == [(z, y, '>')]
assert c.query_order([(y, w)]) == [(y, w, '?')]
assert c.query_order([(y, x)]) == [(y, x, '?')]
assert c.query_order([(x, w)]) == [(x, w, '>')]
assert c.query_order(
    [(w,x),    (w,y),    (w,z),    (x,y),    (x,z),    (y,z),    (z,w),    (z,x),    (z,y),    (y,w),    (y,x),    (x,w)]) \
 == [(w,x,'<'),(w,y,'?'),(w,z,'?'),(x,y,'?'),(x,z,'?'),(y,z,'<'),(z,w,'?'),(z,x,'?'),(z,y,'>'),(y,w,'?'),(y,x,'?'),(x,w,'>')]

assert c.assign_order([(x, z, '>')]) == [(x, z, '>')]
assert c.assign_order([(x, z, '<f')]) == [(x, z, '>')]

assert c.query_order([(w, x)]) == [(w, x, '<')]
assert c.query_order([(w, y)]) == [(w, y, '?')]
assert c.query_order([(w, z)]) == [(w, z, '?')]
assert c.query_order([(x, y)]) == [(x, y, '>')]
assert c.query_order([(x, z)]) == [(x, z, '>')]
assert c.query_order([(y, z)]) == [(y, z, '<')]
assert c.query_order([(z, w)]) == [(z, w, '?')]
assert c.query_order([(z, x)]) == [(z, x, '<')]
assert c.query_order([(z, y)]) == [(z, y, '>')]
assert c.query_order([(y, w)]) == [(y, w, '?')]
assert c.query_order([(y, x)]) == [(y, x, '<')]
assert c.query_order([(x, w)]) == [(x, w, '>')]
assert c.query_order(
    [(w,x),    (w,y),    (w,z),    (x,y),    (x,z),    (y,z),    (z,w),    (z,x),    (z,y),    (y,w),    (y,x),    (x,w)]) \
 == [(w,x,'<'),(w,y,'?'),(w,z,'?'),(x,y,'>'),(x,z,'>'),(y,z,'<'),(z,w,'?'),(z,x,'<'),(z,y,'>'),(y,w,'?'),(y,x,'<'),(x,w,'>')]

assert c.assign_order([(w, z, '<')]) == [(w, z, '<')]
assert c.assign_order([(w, z, '>f')]) == [(w, z, '<')]

assert c.query_order([(w, x)]) == [(w, x, '<')]
assert c.query_order([(w, y)]) == [(w, y, '?')]
assert c.query_order([(w, z)]) == [(w, z, '<')]
assert c.query_order([(x, y)]) == [(x, y, '>')]
assert c.query_order([(x, z)]) == [(x, z, '>')]
assert c.query_order([(y, z)]) == [(y, z, '<')]
assert c.query_order([(z, w)]) == [(z, w, '>')]
assert c.query_order([(z, x)]) == [(z, x, '<')]
assert c.query_order([(z, y)]) == [(z, y, '>')]
assert c.query_order([(y, w)]) == [(y, w, '?')]
assert c.query_order([(y, x)]) == [(y, x, '<')]
assert c.query_order([(x, w)]) == [(x, w, '>')]
assert c.query_order(
    [(w,x),    (w,y),    (w,z),    (x,y),    (x,z),    (y,z),    (z,w),    (z,x),    (z,y),    (y,w),    (y,x),    (x,w)]) \
 == [(w,x,'<'),(w,y,'?'),(w,z,'<'),(x,y,'>'),(x,z,'>'),(y,z,'<'),(z,w,'>'),(z,x,'<'),(z,y,'>'),(y,w,'?'),(y,x,'<'),(x,w,'>')]

try:
    c.assign_order([(x, y, '<')])
    assert False
except RuntimeError:
    pass

assert c.assign_order([(x, y, '<f')]) == [(x, y, '>')]

assert c.query_order([(w, x)]) == [(w, x, '<')]
assert c.query_order([(w, y)]) == [(w, y, '?')]
assert c.query_order([(w, z)]) == [(w, z, '<')]
assert c.query_order([(x, y)]) == [(x, y, '>')]
assert c.query_order([(x, z)]) == [(x, z, '>')]
assert c.query_order([(y, z)]) == [(y, z, '<')]
assert c.query_order([(z, w)]) == [(z, w, '>')]
assert c.query_order([(z, x)]) == [(z, x, '<')]
assert c.query_order([(z, y)]) == [(z, y, '>')]
assert c.query_order([(y, w)]) == [(y, w, '?')]
assert c.query_order([(y, x)]) == [(y, x, '<')]
assert c.query_order([(x, w)]) == [(x, w, '>')]
assert c.query_order(
    [(w,x),    (w,y),    (w,z),    (x,y),    (x,z),    (y,z),    (z,w),    (z,x),    (z,y),    (y,w),    (y,x),    (x,w)]) \
 == [(w,x,'<'),(w,y,'?'),(w,z,'<'),(x,y,'>'),(x,z,'>'),(y,z,'<'),(z,w,'>'),(z,x,'<'),(z,y,'>'),(y,w,'?'),(y,x,'<'),(x,w,'>')]

assert c.assign_order([(w, y, '>')]) == [(w, y, '>')]

assert c.query_order([(w, x)]) == [(w, x, '<')]
assert c.query_order([(w, y)]) == [(w, y, '>')]
assert c.query_order([(w, z)]) == [(w, z, '<')]
assert c.query_order([(x, y)]) == [(x, y, '>')]
assert c.query_order([(x, z)]) == [(x, z, '>')]
assert c.query_order([(y, z)]) == [(y, z, '<')]
assert c.query_order([(z, w)]) == [(z, w, '>')]
assert c.query_order([(z, x)]) == [(z, x, '<')]
assert c.query_order([(z, y)]) == [(z, y, '>')]
assert c.query_order([(y, w)]) == [(y, w, '<')]
assert c.query_order([(y, x)]) == [(y, x, '<')]
assert c.query_order([(x, w)]) == [(x, w, '>')]
assert c.query_order(
    [(w,x),    (w,y),    (w,z),    (x,y),    (x,z),    (y,z),    (z,w),    (z,x),    (z,y),    (y,w),    (y,x),    (x,w)]) \
 == [(w,x,'<'),(w,y,'>'),(w,z,'<'),(x,y,'>'),(x,z,'>'),(y,z,'<'),(z,w,'>'),(z,x,'<'),(z,y,'>'),(y,w,'<'),(y,x,'<'),(x,w,'>')]
