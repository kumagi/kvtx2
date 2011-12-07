# -*- coding: utf-8 -*-
import time
from time import sleep
from random import Random
from random import randint
import memcache
import sys

class AbortException(Exception):
  pass
class ConnectionError(Exception):
  pass

COMMITTED = 'committed'
ABORT = 'abort'
ACTIVE = 'active'

DIRECT = 'direct'
INDIRECT = 'indirect'

def get_committed_value(old, new, status):
  if status == COMMITTED:
    return new
  elif status == ABORT or status == ACTIVE:
    return old
  else:
    raise Exception('invalid status' + status)

def read_committed(old, new, status):
  if status == COMMITTED:
    return new, old
  elif status == ABORT or status == ACTIVE:
    return old, new
  else:
    raise Exception('invalid status' + status)
def read_repeatable(old, new, status):
  if status == COMMITTED:
    return new,old
  elif status == ABORT:
    return old,new
  elif status == ACTIVE:
    return None
  else:
    raise Exception('invalid status' + status)
def async_delete(client, target):
  client.delete(target)

class WrappedClient(object):
  def __init__(self, *args):
    from memcache import Client
    self.mc = Client(*args, cache_cas = True)
    self.del_que = []
    self.random = Random()
    self.random.seed()
    from threading import Thread
    self.del_thread = Thread(target = lambda:self._async_delete())
    self.del_thread.setDaemon(True)
    self.del_thread.start()
  def __getattr__(self, attrname):
    return getattr(self.mc, attrname)
  def _async_delete(self):
    while True:
      try:
        sleep(5)
        while 0 < len(self.del_que):
          target = self.del_que.pop(0)
          if target != None:
            self.mc.delete(target)
      except Exception, e:
        print e
  def add_del_que(self,target):
    self.del_que.append(target)

class MemTr(object):
  """ transaction on memcached """
  def _random_string(self,length):
    string = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'
    ans = ''
    for i in range(length):
      ans += string[self.mc.random.randint(0, len(string) - 1)]
    return ans
  def fetch_by_need(self, key_tuple):
    if key_tuple[0] == DIRECT:
      return key_tuple[1]
    elif key_tuple[0] == INDIRECT:
      return self.mc.get(key_tuple[1])
    else:
      raise Exception("invalid tuple " + str(key_tuple))
  def add_random(self, value):
    length = 8
    while 1:
      key = self.prefix + self._random_string(length)
      result = self.mc.add(key, value)
      if result == True:
        return key
      if not isinstance(result, bool):
        raise ConnectionError
      length += self.mc.random.randint(0, 10) == 0
  def __init__(self, client):
    self.prefix = 'MTP:'
    self.mc = client
  def begin(self):
    self.transaction_status = self.add_random(ACTIVE)
    self.readset = {}
    self.writeset = {}
  def commit(self):
    status = self.mc.gets(self.transaction_status)
    if status != ACTIVE: raise AbortException
    if self.mc.cas(self.transaction_status, COMMITTED):
      # merge readset and writeset
      cache = {}
      for k, v in self.readset.items():
        cache[k] = v[0]
      for k, v in self.writeset.items():
        cache[k] = v
      return cache
    else:
      return None
  class resolver(object):
    def __init__(self, mc):
      self.count = 10
      self.mc = mc
    def __call__(self, other_status):
      sleep(0.001 * randint(0, 1 << self.count))
      if self.count <= 1:
        self.count += 1
      else:
        v = self.mc.gets(other_status)
        if v == COMMITTED: return
        self.count = 0
        self.mc.cas(other_status, ABORT)
  def set(self, key, value):
    resolver = self.resolver(self.mc)
    while 1:
      try:
        #print "set:",self.mc.gets(key)
        old, new, owner = self.mc.gets(key)
      except TypeError:
        if self.mc.add(key,[None, [DIRECT,value], self.transaction_status]):
          self.writeset[key] = value
          break
        time.sleep(1)
        continue
      except ValueError:
        pass
      if owner == self.transaction_status:
        assert(self.writeset.has_key(key))
        if self.mc.get(self.transaction_status) != ACTIVE:
          raise AbortException
        if self.mc.cas(key, [old, [DIRECT,value], self.transaction_status]):
          self.writeset[key] = value
          return
        else:
          raise AbortException
      else:
        state = self.mc.gets(owner)
        if(state == ACTIVE):
          resolver(owner)
          continue
        next_old = get_committed_value(old, new, state)
        if self.writeset.has_key(key):
          raise AbortException
        # delete from readset
        if self.readset.has_key(key):
          fetched_value = self.fetch_by_need(next_old)
          if self.readset[key] != [fetched_value, owner, state]:
            raise AbortException
        result = self.mc.cas(key,[next_old, [DIRECT, value], self.transaction_status])
        if result == True:
          # inheriting success
          self.readset.pop(key, None)
          self.writeset[key] = value
          return
  def get(self, key):
    if self.writeset.has_key(key):
      return self.writeset[key]
    if self.readset.has_key(key):
      return self.readset[key]
    while 1:
      try:
        old, new, owner = self.mc.gets(key)
      except TypeError:
        self.readset[key] = None
        return None
      if owner == self.transaction_status:
        assert(self.writeset.has_key(key))
        return self.writeset[key]
      elif self.writeset.has_key(key):
        raise AbortException
      else:
        state = self.mc.get(owner)
        committed_value = get_committed_value(old, new, state)
        result = self.fetch_by_need(committed_value)
        self.readset[key] = [result, owner, state]
        return result

def rr_transaction(kvs, target_transaction):
  transaction = MemTr(kvs)
  setter = lambda k,v : transaction.set(k,v)
  getter = lambda k :   transaction.get(k)
  while(1):
    transaction.begin()
    try:
      target_transaction(setter, getter)
      result = transaction.commit()
      if result != None:
        return result
    except AbortException:
      continue
    except Exception, e:
      sys.stderr.write("transaction killed " + str(e.message) + "\n")

if __name__ == '__main__':
  mc = WrappedClient(['127.0.0.1:11211'])
  def init(s, g):
    s('counter',0)
  def incr(setter, getter):
    d = getter('counter')
    setter('counter', d+1)
  result = rr_transaction(mc, init)
  from time import time
  begin = time()
  for i in range(10000):
    result = rr_transaction(mc, incr)
  print result['counter']
  print str(10000 / (time() - begin)) + " qps"
