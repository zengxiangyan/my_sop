import json
import sys
import os
import pandas as pd
from . import connect_clickhouse
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
        if not isinstance(message['data'], dict):
            message['data'] = json.loads(message['data'])
        if isinstance(message['data'], list):
            message['data'] = {d['name']: d['value'] for d in message['data']}
        if message['type'] == 'query':
            try:
                if message['data'].get("action") == 'query':
                    try:
                        sql = message['data']['sql']
                        sql = sql.replace(r'@\n@','\n ')
                        print(sql)
                        try:
                            # async with connect_clickhouse.async_connect(0) as session:
                            data = connect_clickhouse.connect(0,sql)
                            print(data)
                            data_json = data.to_json(orient='records')
                            table_data = {}
                            table_data['code'] = 0
                            table_data['msg'] = ""
                            table_data['count'] = len(data_json)
                            table_data['data'] = data_json
                            response = {
                                'type': 'query_result',
                                'sql':sql,
                                'data': table_data
                            }
                            await self.send(json.dumps(response))
                        except Exception as e:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            print(f"Error in {fname} at line {exc_tb.tb_lineno}: 报错类型{exc_type}, {e}")
                            response = {
                                'type': 'Syntax error',
                                'msg': f"Error in {fname} at line {exc_tb.tb_lineno}: {exc_type}, {e}",
                                'data': {}
                            }
                            await self.send(json.dumps(response))
                    except Exception as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        response = {
                            'type': 'Syntax error',
                            'msg': f"Error in {fname} at line {exc_tb.tb_lineno}: {exc_type}, {e}",
                            'data': {}
                        }
                        await self.send(json.dumps(response))
                elif message['data'].get("action") == 'search':
                    form = message['data']
                    sql = connect_clickhouse.sql_create(form=form)
                    # async with connect_clickhouse.async_connect(0) as session:
                    #     cursor = await session.execute(sql)
                    #     fields = cursor._metadata.keys
                    data = connect_clickhouse.async_connect(0,sql)
                    data_json = data.to_json(orient='records')
                    table_data = {}
                    table_data['code'] = 0
                    table_data['msg'] = ""
                    table_data['count'] = len(data_json)
                    table_data['data'] = data_json
                    response = {
                        'type': 'query_result',
                        'sql': sql,
                        'data': table_data
                    }
                    await self.send(json.dumps(response))
                else:
                    response = {
                        'type': 'error',
                        'msg': "未知错误，请联系开发人员",
                        'data': {}
                    }
                    await self.send(json.dumps(response))
            except:
                form = message['data']
                sql = connect_clickhouse.sql_create(form=form)
                # async with connect_clickhouse.async_connect(0) as session:
                data = connect_clickhouse.connect(0,sql)
                data_json = data.to_json(orient='records')
                table_data = {}
                table_data['code'] = 0
                table_data['msg'] = ""
                table_data['count'] = len(data_json)
                table_data['data'] = data_json
                response = {
                    'type': 'query_result',
                    'sql': sql,
                    'data': table_data
                }
                await self.send(json.dumps(response))
