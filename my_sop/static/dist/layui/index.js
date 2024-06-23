layui.use(function(){
  var laydate = layui.laydate;
  // 日期范围
  laydate.render({
    elem: '#date',
    theme: 'molv',
    range: ['#date1', '#date2']
  });
});

// 初始化 CodeMirror
var sqlEditor = CodeMirror(document.getElementById('editor'), {
  mode: 'text/x-sql', // 设置语法模式为 SQL
  theme: 'default', // 设置主题样式
  lineNumbers: true, // 显示行号
  indentUnit: 2, // 缩进单位为 2 个空格
  autofocus: true, // 自动聚焦
  viewportMargin: Infinity // 设置 viewportMargin 为 Infinity
});

// 设置编辑器高度随内容自动调整
function resizeEditor() {
  sqlEditor.setSize(null, 'auto');
}
resizeEditor(); // 初始化时调整一次高度

// 监听编辑器内容变化事件，实时调整高度
sqlEditor.on('change', resizeEditor);

layui.use(['form','table','jquery'], function(){
  var table = layui.table;
  var tableInstance = table.render({
    elem:'#idTest',
    data:[],
    toolbar: true, //仅开启工具栏，不显示左侧模板
    toolbar: '#demoTable',
    totalRow: false, //开启合计行
    page:true,//开启分页
    text:{none:' '},
    request: {
    pageName: 'page', //页码的参数名称，默认：page,
    limitName: 'limit' //每页数据量的参数名，默认：limit
    },
    height:'full-140',
    limit:10,
    limits:[10,20,30,40,50,100,200],
    cols:[],
   });
//   table.on('loading', function() {
//      var loaderHtml = '<div class="loading"></div>';
//      tableInstance.elem.next('.layui-table-box').append(loaderHtml);
//    });
  var url = window.location.href;
  var parser = document.createElement('a');
  parser.href = url;
  var searchParams = new URLSearchParams(parser.search);
  var eid = decodeURIComponent(searchParams.get('eid'));
  var tb = document.getElementById('table').value;
  var socket = new WebSocket(
            'ws://'
            + window.location.host
            + '/ws/sop_e/?eid='
            + eid
        );
  socket.onopen = function() {
  };

  // 监听消息接收事件
  socket.onmessage = function(event) {
    var message = JSON.parse(event.data);
    if (message.type === 'query_result') {
      var sqlreview = message.sql;
      var queryResult = message.data;
      var sqlResult = document.getElementById("sqlResult");
      sqlResult.innerHTML = sqlreview;
      $('#loadingContainer').hide();
      updateTableData(queryResult);
    }else if (message.type === 'error'){
      var msg = message.msg;
      layer.alert(JSON.stringify(message));
    }
  };

  socket.onclose = function(e) {
    console.error('Chat socket closed unexpectedly');
  };
  // 发送查询请求
  function sendQueryRequest(data) {
//    var loading = document.getElementById("loading");
    var queryMessage = {
      type: 'query',
      data: data,
    };
    socket.send(JSON.stringify(queryMessage));
    document.scrollTop = document.scrollHeight;
    var loadingContainer = document.getElementById('loadingContainer');
    loadingContainer.style.position = 'absolute';
    loadingContainer.style.top = '50%';
    loadingContainer.style.left = '50%';
    loadingContainer.style.transform = 'translate(-50%, -50%)';
    $('#loadingContainer').show();
  };

  // 数据库查询返回结果后，根据结果更新表格数据
  function updateTableData(data) {
    var cols = [{type: 'checkbox'}];
    var fields = Object.keys(JSON.parse(data.data)[0]);
    console.log(fields);
    for (var c in fields) {
      if (fields[c] === '销售额' || fields[c] === '销量') {
        cols.push({ field: fields[c], title: fields[c], sort: true });
      } else if ((fields[c] === 'name'|| fields[c] === '宝贝名称'|| fields[c] === '标题') && fields.includes('url')) {
        cols.push({ field: fields[c], title: fields[c], sort: true, templet:'<div><a href="{{d.url}}" target="_blank " style="color:#01AAED" title="点击查看">{{d.name}}</a></div>'});
      } else if (fields[c] === 'name'|| fields[c] === '宝贝名称'|| fields[c] === '标题') {
        cols.push({ field: fields[c], title: fields[c], sort: true, templet:'<div><a href="/#" target="_blank " style="color:#01AAED" title="点击查看">{{d.name}}</a></div>'});
      } else if (fields[c] === '图片'){
        cols.push({ field: fields[c], title: fields[c],width:'5.5%', sort: true,templet:'<div><a href="{{d.图片}}" class="layui-table-link" target="_blank" title="点击查看"><img src="{{d.图片}}" style="max-width:100%;height:60px"></a></div>'});
      } else if (fields[c] === 'img'){
        cols.push({ field: fields[c], title: fields[c],width:'5.5%', sort: true,templet:'<div><a href="{{d.img}}" class="layui-table-link" target="_blank" title="点击查看"><img src="{{d.img}}" style="max-width:100%;height:60px"></a></div>'});
      } else if (fields[c] !== 'url'&& fields[c] !== 'row_num'){
        cols.push({ field: fields[c], title: fields[c], sort: true});
      }
      };
//    cols.push({title:'edit',  toolbar: '#barDemo'});
    tableInstance.reload({
      data: JSON.parse(data.data),
      cols: [cols],
      done: function(res, curr, count) {
        // 绑定工具按钮的点击事件
        bindToolBtnEvent();
      }
    });
    console.log(cols);
    function bindToolBtnEvent() {
    $('.demoTable').off('click', '.layui-btn').on('click', '.layui-btn', function() {
      var type = $(this).data('type');
      if (active[type]) {
        active[type].call(this);
      }
    });
  }
  };
  var form = layui.form;
  var table = layui.table;
  var eid = JSON.parse(document.getElementById('eid').textContent);
  var view_sp = JSON.parse(document.getElementById('view_sp').textContent);
    form.on('submit(submitBtn)', function(data){
    $ = layui.$;
    var source = [];
    $('input[type=checkbox]:checked').each(function() {
      source.push($(this).val());
    });
    // 阻止表单默认提交行为
    event.preventDefault();
    // 获取当前点击的按钮的值
    var action = $(this).attr('value');
    formData = $('#myForm').serializeArray();
    console.log(source);
    formData.push({ name: 'eid', value: eid });
    formData.push({ name: 'action', value: action });
    formData.push({ name: 'table', value: document.getElementById('table').value });
    formData.push({ name: 'source', value: source });
    formData.push({ name: 'view_sp', value: view_sp });
    sendQueryRequest(JSON.stringify(formData, null, 2));
    return false; // 阻止表单提交
    });
      // 监听表单提交事件
   var RunButton = document.getElementById('Run-sqledit');
   var copyButton = document.getElementById('copy-sqledit');
   var copysqlreview = document.getElementById('copy-sqlreview');
   var copysqlResult = document.getElementById('sqlResult');
   var sqlEditor = document.getElementById('editor');
   copysqlreview.addEventListener('click', function(event) {
        var copysqledit = copysqlResult.innerText;
        // 移除行号
        copyToClipboard(copysqledit);
        layer.msg('SQL 语句已复制到剪贴板');
    });
   copyButton.addEventListener('click', function(event) {
        var copysqledit = sqlEditor.innerText;
        // 移除行号
        var lines = copysqledit.split("\n");
        var sqlList = [];
        for (var i = 0; i < lines.length; i++) {
          if(i%2===0){
          lines[i] = lines[i].replace(lines[i],'\n');  // 移除行号
          }else{
          }
        };
        var copytext = lines.join('');
        copyToClipboard(copytext);
        layer.msg('SQL 语句已复制到剪贴板');
    });
    function copyToClipboard(text) {
        var tempInput = document.createElement('textarea');
        tempInput.value = text;
        document.body.appendChild(tempInput);
        tempInput.select();
        document.execCommand('copy');
        document.body.removeChild(tempInput);
  }
   RunButton.addEventListener('click', function(event) {
    　  layer.msg('提交成功');
        var copysqledit = sqlEditor.innerText;
        // 移除行号
        console.log(copysqledit);
        var lines = copysqledit.split("\n");
        var sqlList = [];
        for (var i = 0; i < lines.length; i++) {
          if(i%2!=0){
          sqlList.push(lines[i]);  // 移除行号
          }else{
          }
        };
        copysqledit = sqlList.join('@\\n@');
        console.log(copysqledit);
        var formData = {};
        formData['eid'] =  eid ;
        formData['table'] =  eid ;
        formData['action'] =  'query' ;
        formData['sql'] =  copysqledit ;
        sendQueryRequest(formData);
    });
  //监听表格复选框选择
  table.on('checkbox(demo)', function(obj){
    console.log(obj)
  });
  form.on('submit()', function(data){
    console.log(data.data);
    layui.form.render("select");
  });
  //监听工具条
  table.on('tool(demo)', function(obj){
    var data = obj.data;
    if(obj.event === 'detail'){
      layer.alert(data.img);
    } else if(obj.event === 'del'){
      layer.confirm('确认删除此数据吗？',
        function(){
          obj.del();
          layer.close(index);
      });
    } else if(obj.event === 'edit'){
      layer.alert('编辑行：<br>'+ JSON.stringify(data))
    }
  });
  var $ = layui.$, active = {
    search: function(){
        $("#search0").click();
    },
    reset: function(){
        $("#reset0").click();
        return false;
    },
    getCheckData: function(){ //获取选中数据
        var checkStatus = table.checkStatus('idTest')
        ,data = checkStatus.data;
        layer.alert(JSON.stringify(data));
    }
    ,getCheckLength: function(){ //获取选中数目
        var checkStatus = table.checkStatus('idTest')
        ,data = checkStatus.data;
        layer.msg('选中了：'+ data.length + ' 个');
    }
    ,isAll: function(){ //验证是否全选
        var checkStatus = table.checkStatus('idTest');
        layer.msg(checkStatus.isAll ? '全选': '未全选')
    }
  };
  $('.demoTable .layui-btn').on('click', function(){
    var type = $(this).data('type');
    active[type] ? active[type].call(this) : '';
  });

// 获取 select 元素

});
var select = document.getElementById('table');

  // 监听下拉框变化事件
  select.addEventListener('change', function() {
    // 获取选中的选项的值
    var selectedValue = select.value;

    // 在控制台输出选中的值（可选）
    //  console.log('选中的值：', selectedValue);

    // 更新 table 的值
    // 这里可以根据需要执行其他操作，如发送 AJAX 请求等
    // 以下示例将更新后的值赋给一个隐藏的表单字段，以便在提交表单时传递给后台
    document.getElementById('table').value = selectedValue;
  });
