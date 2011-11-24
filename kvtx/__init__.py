import memcache
import time
from random import Random

class AbortException(Exception):
  pass
class ConnectionError(Exception):
  pass

from memcache import Client
class MemcacheClientModified(Client):
  def add(self, key, value):
    result = Client.add(self,key,value)
    if not isinstance(result, bool):
      raise ConnectionError
    return result
  def gets(self, key): # it never raise
    result = Client.gets(self,key)
    return result
  def cas(self, key, value):
    result = Client.cas(self, key, value)
    if not isinstance(result, bool):
      raise ConnectionError
    return result

status_committed = 'committed'
status_abort  = 'abort'
status_active = 'active'

class TransactionalMemcacheClient(object):
  def __init__(self, *args):
    self.mc = MemcacheClientModified(*args)
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
        time.sleep(5)
        while 0 < len(self.del_que):
          target = self.del_que.pop(0)
          if target != None:
            print 'deleting:' + target
            self.mc.delete(target)
      except Exception, e:
        print e
        exit()
  def add_del_que(self,target):
    self.del_que.append(target)
class MemTr(object):
  def _random_string(self, length):
    string = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'
    ans = ''
    for i in range(length):
      ans += string[self.mc.random.randint(0, len(string) - 1)]
    return ans
  def add_random(self,prefix, value):
    length = 8
    while 1:
      key = prefix + self._random_string(length)
      result = self.mc.add(key, value)
      if result == True:
        return key
      if not isinstance(result, bool):
        raise ConnectionError
      length += self.mc.random.randint(0, 10) == 0
  def __init__(self, client):
    self.mc = client
    self.status_table = 'status:'
  def begin(self):
    self.transaction_status = self.add_random(self.status_table, status_active)
    self.cache = {}
  def commit(self):
    status = self.mc.gets(self.transaction_status)
    if status != 'active': raise AbortException
    return self.mc.cas(self.transaction_status, status_committed)
  class resolver(object):
    def __init__(self, mc):
      self.count = 10
      self.mc = mc
    def __call__(self, other_status):
      sleep(0.001 * randint(0, 1 << self.count))
      if self.count <= 10:
        self.count += 1
        return False
      else:
        self.count = 0
        print 'cas: ', other_status, '-> abort'
        return self.mc.cas(other_status, 'abort')
  def set(self, key, value):
    resolver = self.resolver(self.mc)
    while True:
      try:
        value, owner = self.mc.gets(key)
      except (ValueError,TypeError):
        print self.mc.get(key)
        result = self.mc.add(key, [value, self.transaction_status])
        if result == False:
          continue
        self.cache[key] = value
        return
      if owner == self.transaction_status: # I already have it
        result = self.mc.cas(key, [value, self.transaction_status])
        if result == False:
          raise AbortException
        return
      else: # other transaction has the value
        other_status = self.mc.gets(owner)
        if other_status != status_active:
          self.mc.set(self.backup_table + key, value)
          result = self.mc.cas(key, [value, self.transaction_status])
          if result == False:
            continue
          self.cache[key] = value
          return
        else: # in case of active, resolve
          result = resolve(other_status)
          print "robiing:",result
          if result == True: # robbing success
            old_value = self.mc.get(self.backup_table + key)
            if old_value != None:
              result = self.mc.cas(key, [old_value, self.transaction_status])
            if result == False: # in case of rollback's failure, retry
              continue
            else: # robbing success, delete backup
              self.mc.delete(self.backup_table + key)
  def get_repeatable(self, key):
    pass

def rr_transaction(kvs, target_transaction):
  transaction = MemTr(kvs)
  setter = lambda k,v : transaction.set(k,v)
  getter = lambda k   : transaction.get_repeatable(k)
  while(1):
    transaction.begin()
    try:
      target_transaction(setter, getter)
      if transaction.commit() == True:
        return transaction.cache
    except AbortException:
      continue

if __name__ == '__main__':
  mc = TransactionalMemcacheClient(['127.0.0.1:11211'])
  def init(s, g):
    s('counter',0)
  def incr(setter, getter):
    d = getter('counter')
    print "counter:",d
    setter('counter', d + 1)
  result = rr_transaction(mc, init)
  print result
  assert(result['counter'] == 0)
  for i in range(10000):
    result = rr_transaction(mc, incr)
  print result['counter']
