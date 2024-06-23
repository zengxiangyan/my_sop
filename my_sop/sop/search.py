# sop/consumers.py
import json
from channels.generic.websocket import AsyncWebsocketConsumer

class MyWebSocketConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        query_string = self.scope['query_string'].decode()
        print(query_string)
        await self.accept()

    async def disconnect(self, close_code):
        pass

    async def receive(self, text_data):
        query_string = self.scope['query_string'].decode()
        message = json.loads(text_data)
        if message['type'] == 'query':
            data = json.loads(message['data'])
            form = {d['name']:d['value'] for d in data}
            # 处理查询请求，执行数据库查询等操作
            # 将查询结果发送回前端
            # response = {
            #     'type': 'query_result',
            #     'data': query_result
            # }
            print(form)
            await self.send(json.dumps(data))