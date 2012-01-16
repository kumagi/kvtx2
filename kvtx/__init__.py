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
    self.mc = Client(*args, cache_cas = True)
    self.del_que = []
    self.random = Random()
    self.random.seed()
    import threading
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
  def save_by_need(self, value):
    packed_value = msgpack.packb(value)
    if THRESHOLD  < len(packed_value):
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
      result = self.mc.add(key, value)
      if result == True:
        return key
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
    self.def_thread = Thread(target = self._async_deflate)
    self.def_thread.setDaemon(True)
    self.def_thread.start()

    # asynchronous deletion thread
    self.del_cv = Condition()
    self.del_que = []
    self.del_thread = Thread(target = self._async_delete)
    self.del_thread.setDaemon(True)
    self.del_thread.start()

  def begin(self):
    self.transaction_status = self.add_random([ACTIVE, []])
    self.readset = {}
    self.writeset = {}
    self.out("begin")
  def out(self,string):
    #sys.stderr.write(self.transaction_status + " : " + string + "\n")
    try:
      sys.stderr.write(str(self.transaction_status) + " wb" + str(self.writeset) + " rb" + str(self.readset) +" : " + string + "\n")
    except:
      pass
    pass
  def queue_consume(self, queue, fn):
    try:
      for item in queue:
        fn(item)
    except Exception,e:
      print "cosume exception",e
  def async_work(self, cv, work_queue, exit_flag, fn):
    while True:
      cv.acquire()
      try:
        while((not exit_flag[0]) and len(work_queue) == 0):
          self.out("worker waiting...")
          cv.wait()
        self.out("awake!")
        if(exit_flag[0]):
          self.out("async work end")
          return
        consume_target = work_queue
        work_queue = [] # intialize
      finally:
        cv.release()
      self.queue_consume(consume_target, fn)
    return
  def async_enq(self, cv, work_queue, work):
    cv.acquire()
    work_queue.append(work)
    cv.notify()
    cv.release()
  def _async_delete(self):
    self.async_work(self.del_cv,
                    self.del_que,
                    self.exit_flag,
                    lambda x:self.mc.delete(x))
  def _async_deflate(self):
    self.async_work(self.def_cv,
                    self.def_que,
                    self.exit_flag,
                    lambda x:self.deflate(x))
  def add_del_que(self, target):
    self.out("add del_que " + str(target) + " do")
    self.async_enq(self.del_cv, self.del_que, target)
    self.out("add del_que " + str(target) + " done.")
  def add_def_que(self, target):
    self.out("add def_que " + str(target) + " do")
    self.async_enq(self.def_cv, self.def_que, target)
    self.out("add def_que" + str(target) + " done.")
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
    sys.stderr.write("join done.")
  def __del__(self):
    self.queue_consume(self.def_que, lambda x:self.deflate(x))
    self.queue_consume(self.del_que, lambda x:self.mc.delete(x))
  def deflate(self, owner):
    try:
      now_status, ref_list = self.mc.gets(owner)
    except TypeError:
      pass
    for key in ref_list:
      try:
        inflate, old, new, now_owner = self.mc.gets(key)
        if owner != now_owner or inflate != INFLATE:
          continue # other client may inherit this abort tkvp
        now_status, _ref_list = self.mc.get(now_owner)
        self.out("deflate: owners status is " + str(now_status))
        if(now_status == ABORT):
          self.out("deflate " + str(key) + " => " + str(old))
          if(old[0] == INFLATE):
            self.mc.cas(key, old)
          else:
            self.mc.cas(key, old[1])
          self.delete_by_need(new)
        elif(now_status == COMMITTED):
          self.out("deflate " + str(key) + " => " + str(new))
          if(new[0] == INFLATE):
            self.mc.cas(key, new)
          else:
            self.mc.cas(key, new[1])
          self.delete_by_need(old)
      except TypeError, e:
        print "deflate:exception",str(e)
        pass
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
    if snapshot != self.readset:
      self.mc.cas(self.transaction_status, [ABORT, self.writeset])
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
          #sys.stderr.write("issue cas\n")
          rob_success = self.memtr.mc.cas(other_status, [ABORT, ref_list])
          if rob_success:
            self.memtr.out("robb done from "+str(other_status))
            self.memtr.add_def_que(other_status)
            self.memtr.add_del_que(other_status)
  def set(self, key, value):
    resolver = self.resolver(self)
    if not self.writeset.has_key(key): # add keyname in status for cleanup
      should_active, old_writeset = self.mc.gets(self.transaction_status)
      if should_active != ACTIVE:
        raise AbortException
      result = self.mc.cas(self.transaction_status, [ACTIVE, self.writeset.keys() + [key]])
      if result == False:
        raise AbortException
    # start
    tupled_new = self.save_by_need(value)
    while 1:
      got_value = self.mc.gets(key)
      self.out("set:got_value for "+key+" => "+str(got_value))
      if got_value == None: # not exist
        if self.mc.add(key, [INFLATE, None, tupled_new, self.transaction_status]):
          self.writeset[key] = value
          return
        if self.mc.cas_ids.get(key) != None:
          self.mc.set(key, [INFLATE, None, tupled_new, self.transaction_status])
        time.sleep(5)
        continue
      try:
        if not isinstance(got_value, list): raise TypeError
        inflate, old, new, owner = got_value # load value
        if inflate != INFLATE: raise TypeError
        self.out("set:unpacked:"+str(got_value))
      except TypeError: # deflate state
        if self.writeset.has_key(key):
          self.delete_by_need(tupled_new)
          raise AbortException
        if self.readset.has_key(key):
          if self.readset[key] != got_value:
            self.out("expected:"+ key + " to be "+ str(self.readset[key]) + " but:" + str(got_value))
            self.delete_by_need(tupled_new)
            raise AbortException
        #self.out("set:"+"key ok\n")

        tupled_old = self.save_by_need(got_value)
        result = self.mc.cas(key, [INFLATE, tupled_old, tupled_new, self.transaction_status])
        if result == True:
          self.out("attach and write " +key + " for "+ str(value))
          self.readset.pop(key, None)
          self.writeset[key] = value
          return
        else:
          self.delete_by_need(tupled_old)
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

        if new[0] == INDIRECT:
          self.delete_by_need(tupled_new)
          result = self.mc.replace(new[1], value)
          if result:
            self.writeset[key] = value
            return
          else:
            self.delete_by_need(new)
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
      except TypeError: # deflated state
        self.readset[key] = got_value
        self.out("get:deflate:"+key+" is " + str(got_value))
        return got_value
      assert(owner_name != self.transaction_status)

      # get for TKVP
      self.out("get:inflated state: owner => " + str(self.mc.get(owner_name)))
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
        if self.mc.cas(key, raw_value):
          self.out("get: deflate "+key +" for "+ str(raw_value))
          self.delete_by_need(old)
          self.delete_by_need(new)

def rr_transaction(kvs, target_transaction):
  transaction = MemTr(kvs)
  setter = lambda k,v : transaction.set(k,v)
  getter = lambda k :   transaction.get(k)
  try:
    while(1):
      transaction.begin()
      try:
        target_transaction(setter, getter)
        result = transaction.commit()
        if result != None:
          return result
      except AbortException:
        continue
  finally:
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
