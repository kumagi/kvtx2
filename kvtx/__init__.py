import memcache
import time
import pylru
from random import Random
from random import randint
import sys

class AbortException(Exception):
  pass
class ConnectionError(Exception):
  pass
class ActiveException(Exception):
  pass

"""
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
"""
def get_committed_value(old, new, status):
  if status == 'committed':
    return new
  elif status == 'abort':
    return old
  elif status == 'active':
    raise ActiveException
  else:
    raise Exception('invalid status' + status)
def read_committed(old, new, status):
  if status == 'committed':
    return new, old
  elif status == 'abort' or status == 'active':
    return old, new
  else:
    raise Exception('invalid status' + str(status))
def read_repeatable(old, new, status):
  if status == 'committed':
    return new,old
  elif status == 'abort':
    return old,new
  elif status == 'active':
    return None
  else:
    raise Exception('invalid status' + status)

status_committed = 'committed'
status_abort  = 'abort'
status_active = 'active'

class WrappedClient(object):
  def __init__(self, *args):
    self.mc = memcache.Client(*args) #, behaviors={"cas":True})
    self.del_que = []
    self.random = Random()
    self.random.seed()
    self.unique = pylru.lrucache(1000)
    from threading import Thread
    self.del_thread = Thread(target = lambda:self._async_delete())
    self.del_thread.setDaemon(True)
    self.del_thread.start()
  def gets(self,key):
    result = unique = None
    while True:
      try:
        result_tuple = self.mc.gets(key)
      except RuntimeError:
        time.sleep(0.1)
        continue
      break
    self.unique[key] = unique
    return result
  def cas(self,key, value):
    try:
      result = self.mc.cas(key, value, self.unique[key])
    except KeyError:
      return False # false negative intentionally
    del self.unique[key]
    return result
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
      if not isinstance(result, bool): # network fail
	time.sleep(0.01)
	continue
      if result == True:
	return key
      length += self.mc.random.randint(0, 10) == 0
  def __init__(self, client):
    self.mc = client
    self.status_table = 'status:'
  def begin(self):
    self.transaction_status = self.add_random(self.status_table, status_active)
    self.cache = {}
    self.readset = {}
  def commit(self):
    status = self.mc.gets(self.transaction_status)
    if status != 'active': raise AbortException

    # validate readset
    for k, v in self.readset.iteritems():
      old, new, owner = self.mc.get(k)
      status = mc.get(owner)
      if v != read_committed(old, new, status):
	return False
 
    # commit
    return self.mc.cas(self.transaction_status, status_committed)
  def set(self, key, value):
    resolver = self.resolver(self.mc)
    self.cache[key] = value
    while True:
      try:
	old, new, owner = self.mc.gets(key)
      except (ValueError,TypeError):
	print "notfnound!!"
	result = self.mc.add(key, [None, value, self.transaction_status])
	if result == False:
	  print "false!"
	  try:
	    a,b,c = self.mc.get(key)
	    print "abc: ", a,",", b,",", c
	  except (ValueError,TypeError):
	    print "set"
	    self.mc.set(key, [None, value, self.transaction_status])
	    print "set_done:",self.mc.get(key)
	    return
	  time.sleep(1)
	  continue
	print "set done"
	return
      if owner == self.transaction_status: # I already own it
	result = self.mc.cas(key, [None, value, self.transaction_status])
	if result == False:
	  raise AbortException
	return
      else: # other transaction has the value
	other_status = self.mc.gets(owner)
	if self.readset.has_key(key):
	  if get_committed_value(old, new, other_status) != self.readset[key]:
            raise AbortException
          else:
            del self.readset[key]
	if other_status != status_active:
	  committed_value = get_committed_value(old, new, other_status)
	  result = self.mc.cas(key, [committed_value, value, self.transaction_status])
	  if result == False:
	    continue
	  return
	else: # in case of active, resolve
	  result = resolver(owner)
	  if result == True: # robbing success
	    result = self.mc.cas(key, [old, self.transaction_status])
	    if result == False: # in case of rollback's failure, retry
	      continue
  def get_repeatable(self, key):
    resolver = self.resolver(self.mc)
    while 1:
      old = new = status_name = None
      try:
	#print "get:",self.mc.gets(key)
	old, new, status_name = self.mc.gets(key)
      except ValueError:
        print self.mc.gets(key)
        exit
      except (TypeError,):
	#print "typeerror"
	return None  # read repeatable!!
      if status_name == self.transaction_status: # already have
	#print 'cache hit'
	return self.cache[key]
	#return self.mc.get(new)
      elif self.readset.has_key(key): # robbed by other, abort
	raise AbortException
      else:
	state = self.mc.gets(status_name)
	committed_value = read_committed(old, new, state)
	self.readset[key] = committed_value
	self.cache = committed_value
	return committed_value
  class resolver(object):
    def __init__(self, mc):
      self.count = 10
      self.mc = mc
    def __call__(self, other_status):
      time.sleep(0.01 * randint(0, 1 << self.count))
      if self.count <= 10:
	self.count += 1
	return False
      else:
	self.count = 0
	print 'cas: ', other_status, '-> abort'
	return self.mc.cas(other_status, 'abort')

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
  mc = WrappedClient(['127.0.0.1:11211'])
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
