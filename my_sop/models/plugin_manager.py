import os
import sys
import time
from imp import find_module
from imp import load_module

from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

class PluginManager(type):
    #静态变量配置插件路径
    __PluginPath = join(abspath(dirname(__file__)), 'plugins')
    __AllModels = {}

    #设置插件路径
    @staticmethod
    def setPluginPath(path):
        if os.path.isdir(path):
            PluginManager.__PluginPath = path
        else:
            print ('%s is not a valid path' % path)

    #递归检测插件路径下的所有插件，并将它们存到内存中
    # @staticmethod
    # def loadAllPlugin():
    #     pluginPath = PluginManager.__PluginPath
    #     if not os.path.isdir(pluginPath):
    #         raise Exception('%s is not a directory' % pluginPath)

    #     items = os.listdir(pluginPath)
    #     print(items)
    #     exit()
    #     for item in items:
    #         if os.path.isdir(os.path.join(pluginPath,item)):
    #             PluginManager.__PluginPath = os.path.join(pluginPath,item)
    #             PluginManager.loadAllPlugin()
    #         else:
    #             if item.find('.py')==len(item)-3 and item.endswith('.py') and item != '__init__.py':
    #                 moduleName = item[:-3]
    #                 if moduleName not in sys.modules:
    #                     fileHandle, filePath, dect = find_module(moduleName, [pluginPath])
    #                     try:
    #                         moduleObj = load_module(moduleName, fileHandle, filePath, dect)
    #                         PluginManager.__AllModels[moduleName] = moduleObj.main
    #                     finally:
    #                         if fileHandle : fileHandle.close()


    @staticmethod
    def loadPlugin(name):
        pluginPath = PluginManager.__PluginPath
        if not os.path.isdir(pluginPath):
            raise Exception('%s is not a directory' % pluginPath)

        item = name + '.py'
        if name not in sys.modules and os.path.isfile(pluginPath+'/'+item):
            fileHandle, filePath, dect = find_module(name, [pluginPath])
            try:
                moduleObj = load_module(name, fileHandle, filePath, dect)
                PluginManager.__AllModels[name] = moduleObj.main
                print(item, 'loaded')
            finally:
                if fileHandle : fileHandle.close()


    #获取插件对象。 delete
    @staticmethod
    def getPluginObject(pluginName):
        if len(PluginManager.__AllModels) == 0:
            PluginManager.loadPlugin(pluginName)

        if pluginName in PluginManager.__AllModels:
            return PluginManager.__AllModels[pluginName]()

        # print('plugin not found:', pluginName)
        return None

    #获取插件对象。
    @staticmethod
    def getPlugin(pluginName, defaultPlugin='batch', args=None):
        if pluginName not in PluginManager.__AllModels:
            PluginManager.loadPlugin(pluginName)

        if pluginName in PluginManager.__AllModels:
            return PluginManager.__AllModels[pluginName](args)

        pluginName = defaultPlugin

        if pluginName not in PluginManager.__AllModels:
            PluginManager.loadPlugin(pluginName)

        print(pluginName, 'plugin not found, will use', defaultPlugin)
        return PluginManager.__AllModels[defaultPlugin](args)


class Plugin():
    def __init__(self, obj):
        self.obj = obj

    def start(self):
        pass

    def __log__(self, *args, **kwargs):
        pass

    def __record__(self, bid=0):
        pass
