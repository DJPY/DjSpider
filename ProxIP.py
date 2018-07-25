#!/usr/bin/python3
# -*- coding:utf-8 -*-


from lxml import etree
import random

from ..DjSpider import Spider, UserAgent


#http://www.xicidaili.com/


class Xici(Spider.DjSpider):
    def __init__(self):
        super(Xici, self).__init__(db='redis')
        self.httpURL = 'http://www.xicidaili.com/wt'
        self.httpsURL = 'http://www.xicidaili.com/wn'
        self.head = UserAgent.pc

    def get_ip(self, urlType):
        if urlType == 'http':
            url = self.httpURL
        elif urlType == 'https':
            url = self.httpsURL
        else:
            raise TypeError('http or https')
        head = {}
        head['User-Agent'] = self.head[random.randint(0, 15)]
        resp = self.get(url, headers=head)
        page = resp.content
        root = etree.HTML(page)
        pageNum = root.xpath('//a/text()')[-2]
        print(pageNum)
        n = 0
        for i in range(1, int(pageNum)+1):
            pageUrl = url + '/%d' % i
            print(pageUrl)
            resp = self.get(url, headers=head)
            page = resp.content
            root = etree.HTML(page)
            httpIp = root.xpath('//table[@id="ip_list"]/tr')[1:]
            ip = httpIp[0].xpath('//td[2]/text()')
            port = httpIp[0].xpath('//td[3]/text()')
            with open('prox_http.json', 'a') as f:
                for i in range(len(ip)):
                    ip_list = ip[i] + ':' + port[i]
                    print(ip_list)
                    prox = {}
                    prox['http'] = ip_list
                    self.redis_set_add('Prox_HTTP', prox)
                    n +=1
                    print(n)
                    #f.write(ip_list+'\n')

    def start(self):
        self.thread_pool(self.get_ip('http'))

    def getHttp(self):
        while True:
            val = self.redis_set_pop('Prox_HTTP')
            data=val['http']
            with open('truehttp.json', 'a') as f:
                f.write(data+'\n')
                print(data)

    def getUse(self, prox):
        url = 'http://www.djplayer.top/2018/03/20/51job/'
        resp = self.get(url, proxies=prox)
        if resp.reason == 'OK':
            print('is ok')

    def httpIp(self):
        ip=[]
        with open('truehttp.json', 'r') as f:
            for i in f:
                d = i.replace('\n', '')
                ip.append(d)
        for i in range(len(ip)):
            self.coroutine(self.getUse(ip[i]))


if __name__ == '__main__':
    a=Xici()
    #a.start()
    #a.getHttp()
    #a.getUse()