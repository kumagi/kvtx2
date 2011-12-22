from sys import path
from os.path import dirname
path.append(dirname(__file__) + '/../..')
from kvtx import *
import sys

def incr_test():
  mc = WrappedClient(["127.0.0.1:11211"])
  def init(s, g):
    s('counter',0)
  def incr(setter, getter):
    d = getter('counter')
    setter('counter', d + 1)
  result = rr_transaction(mc, init)
  print result
  assert(result['counter'] == 0)
  for i in range(100):
    result = rr_transaction(mc, incr)
  print result['counter']
import threading
def conflict_test():
  def init(s, g):
    s('value',0)
  def add(s, g):
    for i in range(10):
      s('value', g('value') + i)
    print ("transaction end:"+str(g('value')))
  mc = WrappedClient(["127.0.0.1:11211"])
  result = rr_transaction(mc, init)
  clients = []
  threads = []
  num = 40
  for i in range(num):
    clients.append(WrappedClient(["127.0.0.1:11211"])),
    threads.append(threading.Thread(target = lambda:rr_transaction(clients[i], add)))
    #rr_transaction(clients[i], add)
  for t in threads:
    t.start()
  for t in threads:
    t.join()
  def read(s,g):
    g('value')
  result = rr_transaction(mc, lambda s,g: g('value'))
  print result
  assert(result['value'] == 45 * num)
def double_read_test():
  def save(s,g):
    s("hoge", 21)
  def read(s,g):
    g("hoge")
    g("hoge")
    s("au", 3)
  mc = WrappedClient(["127.0.0.1:11211"])
  result = rr_transaction(mc, save)
  assert(result["hoge"] == 21)
  result = rr_transaction(mc, read)
  assert(result["hoge"] == 21)
  assert(result["au"] == 3)
