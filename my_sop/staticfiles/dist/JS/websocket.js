var socket = new WebSocket(
            'ws://'
            + window.location.host
            + '/ws/sop_e/?eid='
            + eid
        );
socket.onopen = function() {
  // 连接成功后，发送 AJAX 请求的数据
  $.ajax({
    url: './',
    type: 'POST',
    data: {param1: 'value1', param2: 'value2'},
    success: function(response) {
      // 将请求的数据发送给 WebSocket 服务器
      socket.send(JSON.stringify({request: './', data: response}));
    }
  });
};

socket.onmessage = function(event) {
  var response = event.data;
  // 处理从服务器返回的响应
};

socket.onclose = function(event) {
  // 连接关闭时的处理
};