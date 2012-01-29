# -*- coding: utf-8 -*-
import time
from time import sleep
from random import Random
from random import randint
from threading import Thread
import memcache
import sys
import msgpack

class AbortException(Exception):
  pass
class ConnectionError(Exception):
  pass

INFLATE = 'inflate'

COMMITTED = 'committed'
ABORT = 'abort'
ACTIVE = 'active'

THRESHOLD = 10
DIRECT = 'direct'
INDIRECT = 'indirect'

def get_committed_value(old, new, status):
  if status == COMMITTED:
    return new
  elif status == ABORT or status == ACTIVE:
    return old
  else:
    raise Exception('invalid status:' + str(status))
def get_deleting_value(old, new, status):
  if status == COMMITTED:
    return old
  elif status == ABORT:
    return new
  else:
    assert(status != ACTIVE)
    raise Exception('invalid status:' + str(status))

class WrappedClient(object):
  def __init__(self, *args):
    from memcache import Client
    self.mc = Client(*args, cache_cas = True, socket_timeout=10)
    self.del_que = []
    self.random = Random()
    self.random.seed()
    import threading
  def gets(self, key):
    while True:
      result = self.mc.gets(key)
      if isinstance(result, tuple):
        return result[0]
      return result
  def cas(self, key, value):
    try:
      return self.mc.cas(key, value)
    except TypeError:
      return False
  def add(self, key, value):
    result = self.mc.add(key, value)
    if not isinstance(result, bool):
      raise ConnectionError
    return result
  # delegation
  def __getattr__(self, attrname):
    return getattr(self.mc, attrname)

class MemTr(object):
  """ transaction on memcached """
  def _random_string(self,length):
    string = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'
    ans = ''
    for i in range(length):
      ans += string[self.mc.random.randint(0, len(string) - 1)]
    return ans
  @classmethod
  def should_indirect(cls, value):
    packed_value = msgpack.packb(value)
    if THRESHOLD  < len(packed_value):
      return True
    else:
      return False
  def save_by_need(self, value):
    if MemTr.should_indirect(value):
      return [INDIRECT, self.add_random(value)]
    return [DIRECT, value]
  def delete_by_need(self, key_tuple):
    if key_tuple == None:
      return
    if key_tuple[0] == INDIRECT:
      self.add_del_que(key_tuple[1])
  def fetch_by_need(self, key_tuple):
    if key_tuple == None:
      return None
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
      try:
        result = self.mc.add(key, value)
      except ConnectionError:
        sleep(0.1)
        continue
      if result == True:
	return key
      if length < 250:
        length += self.mc.random.randint(0, 10) == 0
  def __init__(self, client):
    self.prefix = 'auau:'
    self.mc = client

    # thread exit flag
    self.exit_flag = [False]

    from threading import Thread
    from threading import Condition

    # asynchronous deflation thread
    self.def_cv = Condition()
    self.def_que = []
    self.def_thread = Thread(target = lambda:
			       self.async_work(self.def_cv,
					       self.def_que,
					       self.exit_flag,
					       lambda x:self.deflate(x)))
    self.def_thread.setDaemon(True)
    self.def_thread.start()

    # asynchronous deletion thread
    self.del_cv = Condition()
    self.del_que = []
    self.del_thread = Thread(target = lambda:
			       self.async_work(self.del_cv,
					       self.del_que,
					       self.exit_flag,
					       lambda x:self.mc.delete(x)))
    self.del_thread.setDaemon(True)
    self.del_thread.start()

  def begin(self):
    self.transaction_status = self.add_random([ACTIVE, []])
    self.readset = {}
    self.indirected_readset = {}
    self.writeset = {}
    self.indirected_writeset = {}
    self.out("begin")
  def out(self,string):
    #sys.stderr.write(self.transaction_status + " : " + string + "\n")
    try:
      #sys.stderr.write(str(self.transaction_status) + " wb" + str(self.writeset) + " rb" + str(self.readset) +" : " + string + "\n")
      pass
    except:
      pass
    pass

  def async_work(self, cv, work_queue, exit_flag, fn):
    while True:
      try:
        cv.acquire()
      except:
        return
      try:
	while((not exit_flag[0]) and len(work_queue) == 0):
	  cv.wait()
	self.out("awake!")
	if(exit_flag[0]):
	  self.out("async work end")
	  return
	consume_target = work_queue
	work_queue = [] # intialize
      except:
        pass # ignore all
      finally:
        try:
          cv.release()
        except: # ignore all
          pass
      try:
        map(fn, consume_target)
      except:
        pass # ignore all
    return
  def async_enq(self, cv, work_queue, work):
    cv.acquire()
    work_queue.append(work)
    cv.notify()
    cv.release()
  def add_del_que(self, target):
    self.async_enq(self.del_cv, self.del_que, target)
  def add_def_que(self, target):
    self.async_enq(self.def_cv, self.def_que, target)
  def exit(self):
    # destroy all asynchronous thread
    self.exit_flag[0] = True
    self.out("memtr exit start")

    self.def_cv.acquire()
    self.exit_flag[0] = True
    self.def_cv.notify()
    self.def_cv.release()
    self.def_thread.join()

    self.del_cv.acquire()
    self.exit_flag[0] = True
    self.del_cv.notify()
    self.del_cv.release()
    self.del_thread.join()

    self.out("def_que:" + str(self.def_que))
    map(lambda x:self.deflate(x), self.def_que)
    self.out("del_que:" + str(self.del_que))
    map(lambda x:self.mc.delete(x), self.del_que)
  def deflate(self, owner):
    try:
      now_status, ref_list = self.mc.gets(owner)
    except TypeError:
      return
    for key in ref_list:
      try:
	inflate, old, new, now_owner = self.mc.gets(key)
	if owner != now_owner or inflate != INFLATE:
	  continue # other client may inherit this abort tkvp
	now_status, _ref_list = self.mc.get(now_owner)
	self.out("deflate: owners status is " + str(now_status))
	if(now_status == ABORT):
	  self.out("deflate " + str(key) + " => " + str(old))
	  if(old[0] == INDIRECT):
	    self.mc.cas(key, old)
	  else:
	    self.mc.cas(key, old[1])
	  self.delete_by_need(new)
	elif(now_status == COMMITTED):
	  self.out("deflate " + str(key) + " => " + str(new))
	  if(new[0] == INDIRECT):
	    self.mc.cas(key, new)
	  else:
	    self.mc.cas(key, new[1])
	  self.delete_by_need(old)
      except TypeError, e:
	print "deflate:exception",str(e)
	pass
      except Exception,e:
	sys.stderr.write(str(e))
    self.out("deflate deleting owner [" + owner + "]")
    self.add_del_que(owner)

  def commit(self):
    self.out("trycommit")
    try:
      my_status, ref_list = self.mc.gets(self.transaction_status)
    except TypeError: # deleted by other
      self.out("commit fail. because deleted")
      raise AbortException
    if my_status != ACTIVE:
      self.out("commit fail.")
      raise AbortException

    # snapshot check
    snapshot = self.mc.get_multi(self.readset.keys())
    self.out(str(snapshot) +" =?= "+str(self.readset))
    for indirect_key in self.indirected_readset.keys():
      if not isinstance(snapshot[indirect_key], list) or snapshot[indirect_key][1] != self.indirected_readset[indirect_key]:
	self.out("snapshot didnt match for " + str(self.indirected_readset[indirect_key]) + " != " + str(snapshot[indirect_key][1]))
	raise AbortException
      else:
	snapshot.pop(indirect_key,None)
    for rest_key in snapshot.keys():
      if self.readset[rest_key] != snapshot[rest_key]:
	self.mc.cas(self.transaction_status, [ABORT, self.writeset])
	self.out("snapshot didnt match for " + str(self.readset[rest_key]) + " != " + str(snapshot[rest_key]))
	raise AbortException

    if not self.mc.cas(self.transaction_status, [COMMITTED, ref_list]):
      return None # commit fail
    else: # success
      self.out("commit success.")
      self.add_def_que(self.transaction_status)
      self.out("add queue done.")
      # merge readset and writeset for answer
      cache = dict(self.readset.items() + self.writeset.items())
      return cache
  class resolver(object):
    def __init__(self, memtr):
      self.count = 0
      self.memtr = memtr
    def __call__(self, other_status):
      while True:
	sleep(0.001 * randint(0, 1 << self.count))
	try:
	  status, ref_list = self.memtr.mc.gets(other_status)
	except TypeError:
	  return
	if status != ACTIVE:
	  return
	if self.count <= 10:
	  self.count += 1
	  continue
	else:
	  self.count = 0
	  rob_success = self.memtr.mc.cas(other_status, [ABORT, ref_list])
	  if rob_success:
	    self.memtr.out("robb done from "+str(other_status))
	    self.memtr.add_def_que(other_status)
  def set(self, key, value):
    resolver = self.resolver(self)
    if not self.writeset.has_key(key): # add keyname in status for cleanup
      try:
        should_active, old_writeset = self.mc.gets(self.transaction_status)
        if should_active != ACTIVE:
          raise AbortException
        result = self.mc.cas(self.transaction_status, [ACTIVE, self.writeset.keys() + [key]])
        if result == False:
          raise AbortException
      except TypeError:
        raise AbortException
    # start
    tupled_new = self.save_by_need(value)
    while(True):
      got_value = self.mc.gets(key)
      self.out("set:got_value for "+key+" => "+str(got_value))
      if got_value == None: # not exist
	if self.mc.add(key, [INFLATE, None, tupled_new, self.transaction_status]):
	  self.writeset[key] = value
	  return
	if self.mc.cas_ids.get(key) != None:
	  self.mc.set(key, [INFLATE, None, tupled_new, self.transaction_status])
	time.sleep(0.5)
	continue
      try:
	if not isinstance(got_value, list): raise TypeError
	inflate, old, new, owner = got_value # load value
	if inflate != INFLATE: raise TypeError
	self.out("set:unpacked:"+str(got_value))
      except (TypeError, ValueError): # deflate state
	if self.writeset.has_key(key):
	  self.delete_by_need(tupled_new)
	  raise AbortException
	if self.readset.has_key(key):
	  if self.readset[key] != got_value:
	    self.out("expected:"+ key + " to be "+ str(self.readset[key]) + " but:" + str(got_value))
	    self.delete_by_need(tupled_new)
	    raise AbortException
	self.out("set:got_value is " + str(got_value) + "\n")

	if(isinstance(got_value, list) and got_value[0] == INDIRECT):
	  self.out("old owner is deflated and indirected")
	  pass
	else:
	  got_value = [DIRECT, got_value]
	result = self.mc.cas(key, [INFLATE, got_value, tupled_new, self.transaction_status])
	if result == True:
	  self.out("attach and write " +key + " for "+ str(value))
	  self.readset.pop(key, None)
	  self.indirected_readset.pop(key, None)
	  self.writeset[key] = value
	  return
	else:
	  continue
      assert(inflate == INFLATE)

      if owner == self.transaction_status:
	assert(self.writeset.has_key(key))
	try:
	  owner_status, ref_list = self.mc.get(self.transaction_status)
	  if owner_status != ACTIVE:
	    raise TypeError
	except TypeError: # if deleted
	  self.delete_by_need(tupled_new)
	  raise AbortException

	self.out("I am owner " + str(new))
	if new[0] == INDIRECT:
	  result = self.mc.replace(new[1], value)
	  if result:
	    self.out("success to replace " + str(new))
	    self.delete_by_need(tupled_new)
	    self.writeset[key] = value
	    return
	  else:
	    self.delete_by_need(tupled_new)
	    raise AbortException
	if self.mc.cas(key, [INFLATE, old, tupled_new, self.transaction_status]):
	  self.writeset[key] = value
	  self.delete_by_need(new)
	  return
	else: # other thread should rob
	  self.delete_by_need(new)
	  self.delete_by_need(tupled_new)
	  raise AbortException
      else:
	self.out( " != " + str(owner))
	try:
	  self.out("set: old:"+str(old)+ " new:"+str(new)+" "+str(self.mc.get(owner)))
	  state, ref_list = self.mc.gets(owner)
	except TypeError: # other thread push to deflate state
	  try:
	    inflate, _1, _2, second_owner_name = self.mc.gets(key)
	    if inflate == INFLATE and owner == second_owner_name: # killed owner inflated this
	      committed_value = get_committed_value(old, new, ABORT)
	      self.delete_by_need(new)
	      raw_value = self.fetch_by_need(old)
	      self.mc.cas(key, raw_value)
	  except TypeError:
	    pass
	  continue
	if self.writeset.has_key(key): # robbed check
	  self.delete_by_need(tupled_new)
	  raise AbortException # it means that kvp robbed by other
	if(state == ACTIVE):
	  resolver(owner)
	  continue

	assert(state == COMMITTED or state == ABORT)
	next_old = get_committed_value(old, new, state)
	to_delete = get_deleting_value(old, new, state)
	if self.readset.has_key(key): # validate consitency with readset
	  if self.indirected_readset.has_key(key):
	    if(not isinstance(next_old, list) or
	       self.indirected_readset[key] != next_old[1]):
	      raise AbortException
	  fetched_value = self.fetch_by_need(next_old)
	  if self.readset[key] != fetched_value:
	    self.delete_by_need(tupled_new)
	    raise AbortException # incoherence value detected

	# do inherit
	inherit_result = self.mc.cas(key,[INFLATE, next_old, tupled_new, self.transaction_status])
	if inherit_result == True: # inheriting success
	  self.out("success to attach " + key)
	  self.delete_by_need(to_delete)
	  self.readset.pop(key, None)
	  self.writeset[key] = value
	  self.add_def_que(owner)
  def get(self, key):
    resolver = self.resolver(self)
    if self.writeset.has_key(key):
      return self.writeset[key]
    if self.readset.has_key(key): # repeat read
      return self.readset[key]
    while 1:
      try:
	got_value = self.mc.gets(key)
	self.out("get:got_value for "+key+" => "+str(got_value))
	inflate, old, new, owner_name = got_value
	if inflate != INFLATE: # deflate state
	  self.out("not inflated")
	  raise TypeError
      except (TypeError, ValueError): # deflated state
	if(isinstance(got_value, list) and got_value[0] == INDIRECT):
	  self.indirected_readset[key] = got_value[1]
	  got_value = self.mc.get(got_value[1])
	self.readset[key] = got_value
	self.out("get:deflate:"+key+" is " + str(got_value))
	return got_value
      assert(owner_name != self.transaction_status)

      # get for TKVP
      try:
	state, _ = self.mc.gets(owner_name) # ref_list will be ignored
      except TypeError: # status deleted? it may became deflated state
	try:
	  inflate, _1, _2, second_owner_name = self.mc.gets(key)
	  if inflate == INFLATE and owner_name == second_owner_name:
	    # killed owner of this
	    committed_value = get_committed_value(old, new, ABORT)
	    self.delete_by_need(new)
	    raw_value = self.fetch_by_need(old)
	    self.mc.cas(key, raw_value)
	except TypeError:
	  pass
	continue

      self.out("inherit from inflated "+ str(key))
      committed_value = get_committed_value(old, new, state)
      raw_value = self.fetch_by_need(old)
      if raw_value == None:
	if got_value != self.mc.gets(key):
	  continue
      if state == ACTIVE:
	resolver(owner_name)
      else:
	raw_value = self.fetch_by_need(committed_value)
	self.add_def_que(owner_name)
	if self.mc.cas(key, raw_value):
	  self.out("get: deflate "+key +" for "+ str(raw_value))
	  self.delete_by_need(old)
	  self.delete_by_need(new)

def rr_transaction(kvs, target_transaction, clean = False):
  transaction = MemTr(kvs)
  setter = lambda k,v : transaction.set(k,v)
  getter = lambda k :	transaction.get(k)
  wait_count = 1
  try:
    while(1):
      transaction.begin()
      try:
	target_transaction(setter, getter)
	result = transaction.commit()
	if result != None:
	  return result
      except AbortException:
	transaction.out("aborted:" + str(transaction.transaction_status))
	transaction.add_def_que(transaction.transaction_status)
	continue
      except ConnectionError:
        transaction.add_def_que(transaction.transaction_status)
        sleep(0.001 * randint(0, 1 << wait_count))
        if wait_count < 10:
          wait_count += 1
        continue
  finally:
    if clean:
      transaction.exit()

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
