# -*- coding: utf-8 -*-
import time
from time import sleep
from random import Random
from random import randint
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
        sleep(0.01)
        while 0 < len(self.del_que):
          target = self.del_que.pop(0)
          if target != None:
            self.mc.delete(target)
      except Exception, e:
        print e
  def add_del_que(self,target):
    self.delete(target)
    return
    self.del_que.append(target)
  def __del__(self):
    for key in self.del_que:
      self.mc.delete(key)

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
    if key_tuple[0] == INDIRECT:
      self.mc.add_del_que(key_tuple[1])
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
        sys.stderr.write("add:"+str(key)+"=>"+str(value)+"\n")
        return key
      if not isinstance(result, bool):
        raise ConnectionError
      length += self.mc.random.randint(0, 10) == 0
  def __init__(self, client):
    self.prefix = 'MTP:'
    self.mc = client
  def begin(self):
    self.transaction_status = self.add_random([ACTIVE, []])
    self.readset = {}
    self.writeset = {}
  def commit(self):
    try:
      my_status, ref_list = self.mc.gets(self.transaction_status)
    except TypeError: # deleted by other
      raise AbortException
    if my_status != ACTIVE: raise AbortException
    if not self.mc.cas(self.transaction_status, [COMMITTED, ref_list]):
      return None # commit fail
    else: # success
      # deflate the inflated kvp
      for key in self.writeset.keys():
        try:
          inflate, old, new, status = self.mc.gets(key)
          if status != self.transaction_status:
            continue # other client may inherit the abort kvp
          self.mc.cas(key, self.writeset[key]) # deflation
          self.delete_by_need(new)
          self.delete_by_need(old)

          # NOTICE: cas fail does mean inherited by other, we should do nothing
        except TypeError: # already deflated by other
          pass
      # delete my status, no other object may point this status as owner
      self.mc.add_del_que(self.transaction_status)

      # merge readset and writeset for answer
      cache = {}
      for k in self.readset.keys():
        cache[k] = self.readset[k][0]
      for k in self.writeset.keys():
        cache[k] = self.writeset[k]
      return cache
  class resolver(object):
    def __init__(self, memtr):
      self.count = 0
      self.memtr = memtr
    def __call__(self, other_status):
      sleep(0.001 * randint(0, 1 << self.count))
      if self.count <= 5:
        self.count += 1
      else:
        #sys.stderr.write("try rob:"+str(self.mc.get(other_status))+"\n")
        try:
          status, ref_list = self.memtr.mc.gets(other_status)
        except TypeError:
          return
        if status == COMMITTED: return
        self.count = 10
        #sys.stderr.write("issue cas\n")
        rob_success = self.memtr.mc.cas(other_status, [ABORT, ref_list])
        if rob_success:
          sys.stderr.write("robb done from "+str(other_status) + "\n")
          # deflate the inflated kvp
          for key in ref_list:
            try:
              got_value = self.memtr.mc.gets(key)
              inflate, old, new, owner_name = got_value
              if owner_name != other_status:
                continue
              self.memtr.delete_by_need(new)
              commited_value = self.memtr.fetch_by_need(old)
              casresult = self.memtr.mc.cas(key, commited_value) # deflation
              if casresult:
                self.memtr.delete_by_need(old)
          # NOTICE: cas fail does mean inherited by other, we should do nothing
            except TypeError:
              pass
          self.memtr.mc.add_del_que(other_status)
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
      #sys.stderr.write("set:got_value:"+str(got_value)+" "+self.transaction_status+"\n")
      if got_value == None: # not exist
        if self.mc.add(key, [INFLATE, None, tupled_new, self.transaction_status]):
          self.writeset[key] = value
          return
        time.sleep(0.5)
        continue
      try:
        inflate, old, new, owner = got_value # load value
        if inflate != INFLATE: raise TypeError
      except TypeError: # quisine state
        if self.writeset.has_key(key):
          self.delete_by_need(tupled_new)
          raise AbortException
        if self.readset.has_key(key):
          if self.readset[key][0] != got_value or self.readset[key][2] == ABORT:
            self.delete_by_need(tupled_new)
            raise AbortException
        #sys.stderr.write("set:"+"key ok\n")

        tupled_old = self.save_by_need(got_value)
        result = self.mc.cas(key, [INFLATE, tupled_old, tupled_new, self.transaction_status])
        if result == True:
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
        #sys.stderr.write(str(self.transaction_status) + " != " + owner + "\n")
        try:
          sys.stderr.write("old:"+str(old)+ " new:"+str(new)+" "+str(self.mc.get(owner))+"\n")
          state, ref_list = self.mc.gets(owner)
        except TypeError: # other thread push to quisine state
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
          if self.readset[key] != [fetched_value, owner, state]:
            self.delete_by_need(tupled_new)
            raise AbortException # incoherence value detected

        # do inherit
        inherit_result = self.mc.cas(key,[INFLATE, next_old, tupled_new, self.transaction_status])
        if inherit_result == True: # inheriting success
          self.delete_by_need(to_delete)
          self.readset.pop(key, None)
          self.writeset[key] = value
          return
  def get(self, key):
    if self.writeset.has_key(key):
      return self.writeset[key]
    if self.readset.has_key(key): # repeat read
      return self.readset[key][0]
    while 1:
      try:
        got_value = self.mc.gets(key)
        #sys.stderr.write("get:got_value:"+str(got_value)+"\n")
        inflate, old, new, owner_name = got_value
        if inflate != INFLATE: # quisine state
          raise TypeError
      except TypeError: # deflated state
        self.readset[key] = [got_value, None, None]
        return got_value
      assert(owner_name != self.transaction_status)

      # get
      try:
        state, _ = self.mc.gets(owner_name) # ref_list will be ignored
      except TypeError:
        continue

      committed_value = get_committed_value(old, new, state)
      raw_value = self.fetch_by_need(old)
      if raw_value == None:
        if got_value != self.mc.gets(key):
          continue
      if state == ACTIVE:
        self.readset[key] = [raw_value, owner_name, ACTIVE]
        return raw_value
      else:
        if self.mc.cas(key, raw_value):
          self.delete_by_need(old)
          self.delete_by_need(new)

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
      #sys.stderr.write("...aborted\n")
      continue

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
