from TorCtl import TorCtl
import urllib2
from time import sleep as slp

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

def get_data(url):
        resp_len = None
        while not resp_len:
                try:
                        renew_connection()
                        resp =  request(url)
                        resp_len = len(resp)
                except:
                        slp(2)
                pass
        return resp


#data = get_data("http://www.indeed.com/resumes/Boxer?co=US&rb=dt%3Adi")
#print len(data)