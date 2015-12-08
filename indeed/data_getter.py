import sys, os
from TorCtl import TorCtl
import urllib2
from time import sleep as slp, time as tm
from subprocess import call

user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
headers={'User-Agent':user_agent}

def request(url):
    def _set_urlproxy():
        proxy_support = urllib2.ProxyHandler({"http" : "127.0.0.1:8118"})
        opener = urllib2.build_opener(proxy_support)
        urllib2.install_opener(opener)
        return

    _set_urlproxy()
    request = urllib2.Request(url, None, headers)
    return urllib2.urlopen(request).read()

def renew_connection():
    conn = TorCtl.connect(controlAddr="127.0.0.1", controlPort=9051, passphrase="your_password")
    conn.send_signal("NEWNYM")
    conn.close()

def reset_():
    comp = os.environ['recent_']
    comp = comp.split(' ')
    index = int(comp[0])
    status = comp[1]

    #--it began, but got stuck sadly
    if status == 'begin':
        index = index-1
    #--it finished properly or was interrupted in the middle..doesn't matter..consider the keyword being over..
    if status == 'end':
        index = index

    #--stop the tor and associated stuff manually
    commands = ["/etc/init.d/tor stop", "/etc/init.d/privoxy stop"]
    for each in commands:
        call([each], shell=True)

    command = "bash restart.sh %d" % index
    execute = call([command], shell=True)

def get_data(url, index=False):
    if index:
        reset_()

    t1 = tm()
    resp_len = None
    stuck = False
    while not resp_len:
        if (tm() - t1) >= 3*60:
            stuck = True
            resp_len = 1
        try:
            renew_connection()
            resp =  request(url)
            resp_len = len(resp)
        except:
            slp(5)
            pass
    if stuck:
        #print "~~RESETTING~~"
        #reset_()
        return 1

    return resp


#data = get_data("http://www.indeed.com/resumes/Boxer?co=US&rb=dt%3Adi")
#print len(data)
