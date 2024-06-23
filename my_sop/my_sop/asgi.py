# -*- coding: utf-8 -*-
import os

from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack
import sop.routing

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'my_sop.settings')

application = ProtocolTypeRouter({

    '''
        This root routing configuration specifies that 
        when a connection is made to the Channels development server,
        the ProtocolTypeRouter will first inspect the type of connection. 
        If it is a WebSocket connection (ws:// or wss://), 
        the connection will be given to the AuthMiddlewareStack.
    '''

    "http": get_asgi_application(),

    # 在这里指定websocket协议交由特定的路由来处理
    "websocket": AuthMiddlewareStack(
        URLRouter(
            sop.routing.websocket_urlpatterns
        )
    )
})
