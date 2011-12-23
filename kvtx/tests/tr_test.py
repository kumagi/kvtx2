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
  num = 1
  for i in range(num):
    clients.append(WrappedClient(["127.0.0.1:11211"])),
    threads.append(threading.Thread(target = lambda:rr_transaction(clients[i], add)))
    #rr_transaction(clients[i], add)
  for t in threads:
    t.setDaemon(True)
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
def many_account_transaction_test():
  accounts = 100
  first_money = 1000
  def init(setter,getter):
    for i in range(accounts):
      setter("account:"+str(i), first_money)
  mc1 = WrappedClient(["127.0.0.1:11211"])
  rr_transaction(mc1, init)
  def checker(setter,getter):
    account = []
    for i in range(accounts):
      account.append(getter("account:"+str(i)))
  import random
  def work(mc):
    for i in range(1000):
      from_account = random.randint(0,accounts-1)
      to_account = random.randint(0,accounts-1)
      if from_account == to_account:
        continue
      money = 1#random.randint(0,first_money)
      def move(setter,getter):
        from_money = getter("account:"+str(from_account))
        if from_money < money:
          #print "from_money:"+str(from_money)
          return
        to_money = getter("account:"+str(to_account))
        setter("account:"+str(from_account), from_money - money)
        setter("account:"+str(to_account), to_money + money)
      result = rr_transaction(mc, move)
      #sys.stderr.write(str(result["account:"+str(from_account)]))
      assert(0 < result["account:"+str(from_account)])

      result = rr_transaction(mc, checker)
      total = 0
      for i in range(accounts):
        total += result["account:"+str(i)]
      #sys.stderr.write("middle result :"+str(total) + " expect " + str(accounts * first_money) + " in "+str(from_account)+"->"+str(to_account) + "dump"+str(result))
      assert(total == accounts * first_money)
  clients = []
  threads = []
  num = 10
  for i in range(num):
    clients.append(WrappedClient(["127.0.0.1:11211"]))
    threads.append(threading.Thread(target = lambda:work(clients[i])))
    #work(clients[i])
  for t in threads:
    t.setDaemon(True)
    t.start()
  for t in threads:
    t.join()
  mc2 = WrappedClient(["127.0.0.1:11211"])
  result = rr_transaction(mc2, checker)

  total = 0
  for i in range(accounts):
    total += result["account:"+str(i)]
  print "result :"+str(total) + " expect " + str(accounts * first_money)
  assert(total == accounts * first_money)

def large_value_test():
  def init(s,g):
    s("long", "hoge"*100)
  def two_write(s,g):
    s("auaua", "hoge"*100)
    s("auaua", "aua"*200)
  mc = WrappedClient(["127.0.0.1:11211"])
  result = rr_transaction(mc, init)
  assert(result["long"] == "hoge"*100)
  result = rr_transaction(mc, two_write)
  assert(result["auaua"] == "aua"*200)
