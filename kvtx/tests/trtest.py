from sys import path
from os.path import dirname
path.append(dirname(__file__) + '/..')
from kvtx import *
import sys

def incr_test():
  mc = WrappedClient(["127.0.0.1:11211"])
  def init(s, g):
    s('counter',0)
  def incr(setter, getter):
    d = getter('counter')
    print "counter:",d
    setter('counter', d + 1)
  result = rr_transaction(mc, init)
  assert(result['counter'] == 0)
  for i in range(10000):
    print i
    result = rr_transaction(mc, incr)
  print result['counter']
