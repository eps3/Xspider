#!/usr/bin/env python
# -*- coding:utf-8 -*-

import Queue
import logging
import time
import threading
from multiprocessing.managers import BaseManager

logging.basicConfig(level=logging.INFO,
                    format='%(filename)s %(asctime)s %(thread)d [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


class QueueManage(object):
    sys_queue = Queue.Queue()
    """
    队列管理器
    """

    def __init__(self, host='', port=19002, authkey='', debug=False):
        if debug:
            logger.setLevel(logging.DEBUG)
        self.host = host
        self.port = port
        self.authkey = authkey
        self.manage = None
        self.queue_list = {}

    def add_queue(self, queue_name, queue_):
        """
        添加队列
        :param queue_name: 队列名称
        :param queue_: 队列 Queue.Queue类型
        """
        self.queue_list[queue_name] = None
        BaseManager.register(queue_name, callable=lambda: queue_)
        logger.debug('register queue %s ok !' % queue_name)
        return self

    def add_queue_name(self, queue_name):
        """
        客户端添加队列名称
        :param queue_name: 队列名称
        """
        self.queue_list[queue_name] = None
        BaseManager.register(queue_name)
        logger.debug('register queue %s ok !' % queue_name)
        return self

    def _load_queue(self):
        """
        从分布式环境获取队列
        """
        for queue_name in self.queue_list:
            self.queue_list[queue_name] = getattr(self.manage, queue_name)()
        return self

    def start(self):
        """
        启动队列管理器
        """
        self.add_queue('get_sys_queue', self.sys_queue)
        self.manage = BaseManager(address=(self.host, self.port), authkey=self.authkey)
        logger.debug('start queue manage.....')
        self.manage.start()
        logger.debug('start queue manage ok !')
        # 加载分布式队列
        self._load_queue()
        while True:
            try:
                msg = self.queue_list['get_sys_queue'].get()
                logger.info('get system queue message: %s' % msg)
                if msg == 'shutdown':
                    break
            except Exception, e:
                logger.error('et system queue message error %s' % str(e))
        self.join()
        self._shutdown()

    def join(self):
        """
        等待所有队列完成
        """
        logger.debug('join all queue !')
        for queue_name in self.queue_list:
            if queue_name != 'get_sys_queue':
                logger.debug('waiting %s ......' % queue_name)
                # self.queue_list[queue_name].join()
                while not self.queue_list[queue_name].empty():
                    time.sleep(2)
        logger.info('all queue have done!')
        return self

    def _shutdown(self):
        """
        关闭管理器
        """
        self.manage.shutdown()

    def _connect(self):
        """
        连接管理器
        """
        if not self.manage:
            self.add_queue_name('get_sys_queue')
            self.manage = BaseManager(address=(self.host if self.host else '127.0.0.1', self.port),
                                      authkey=self.authkey)
            self.manage.connect()
            self._load_queue()
            logger.debug('connect %s:%s ok!' % (self.host, self.port))
        return self

    def put(self, message, queue_name='get_sys_queue'):
        """
        向队列发送消息
        :param message: 消息内容
        :param queue_name: 队列名称
        """
        self._connect()
        self.queue_list[queue_name].put(message)
        logger.debug('put to %s:%s:%s ok' % (self.host, self.port, queue_name))
        return self

    def get(self, queue_name='get_sys_queue'):
        """
        取出队列中的一条消息
        :param queue_name: 队列名称
        """
        self._connect()
        message = self.queue_list[queue_name].get()
        logger.debug('get to %s:%s:%s: %s' % (self.host, self.port, queue_name, message))
        return message


class Spider(object):
    def __init__(self, name='spider_kid'):
        self.name = name
        self.task_list = []
        self._thread_list = []

    def add_task(self, task, thread_count=10):
        """
        添加任务及线程数
        :param task: 任务－> 方法
        :param thread_count: 线程数
        """
        self.task_list.append((task, thread_count))
        return self

    def start(self):
        """
        启动任务
        """
        for task, thread_count in self.task_list:
            for i in range(0, thread_count):
                _thread = threading.Thread(target=task)
                _thread.setDaemon(True)
                _thread.start()
                self._thread_list.append(_thread)
        return self

    def join(self):
        """
        等待任务完成
        :return:
        """
        for _thread in self._thread_list:
            _thread.join()
        return self
