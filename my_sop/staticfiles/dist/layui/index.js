<<<<<<< HEAD
layui.use(['form', 'table', 'jquery', 'laydate', 'layer', 'util'], function() {
    var $ = layui.jquery;
    var form = layui.form;
    var table = layui.table;
    var laydate = layui.laydate;
    var layer = layui.layer;
    var util = layui.util;
    var allData = [];

    active = {
        search: function(){
            $("#search0").click();
        },
        reset: function(){
            $("#reset0").click();
            return false;
        },
        set_view_sp: function(){
            var xhr = new XMLHttpRequest();
            var eid = JSON.parse(document.getElementById('eid').textContent);
            var tb = document.getElementById('table').value;
            window.location.href = window.location.protocol + '//' + window.location.host + '/sop_e/set_view_sp/?eid=' + eid + '&tb=' + tb;
        },
        exportAll: function(obj){
            console.log(allData);
            console.log(obj)
            if (allData.length > 0) {
                    table.exportFile('currentTableId', allData, 'csv');
                } else {
                    layer.msg('没有数据可以导出');
                }
        }
        ,save: function(){
            layer.confirm('<input type="text" id="recordName" class="layui-input" style="width:600px">', {
                    title: '请输入记录名',
                    btn: ['提交', '关闭']
                }, function(index, layero){
                    var inputValue = $('#recordName').val();
                    if(inputValue === '') {
                        $('#recordName').focus();
                        return false; // 阻止对话框关闭
                    }
                    layer.msg('获得：' + inputValue);
                    layer.close(index);
                });
        }
      };

    // 初始化日期选择器
    laydate.render({
        elem: '#date',
        theme: 'molv',
        range: ['#date1', '#date2']
    });

    // 初始化表格
    table.render({
        elem: '#currentTableId',
        id: 'currentTableId',
        title: JSON.parse(document.getElementById('eid').textContent),
        height: 748,
        cols: [],
        data: [],
        page: true,
        limit: 15,
        limits: [10, 15, 20, 25, 50, 100],
        skin: 'grid',
        even: true,
        toolbar: '#demoTable',
        defaultToolbar: [],
        done: function(res, curr, count) {
            bindToolBtnEvent();
        }
    });

    function getCSRFToken() {
        var cookies = document.cookie.split(';');
        for (var i = 0; i < cookies.length; i++) {
            var cookie = cookies[i].trim();
            var cookieParts = cookie.split('=');
            if (cookieParts[0] === 'csrftoken') {
                return cookieParts[1];
            }
        }
        return null;
    }

    function sendQueryRequest(data) {
        var index = layer.load(1, {shade: [0.1, '#fff']});
        var formData = JSON.parse(data);
        formData['csrfmiddlewaretoken'] = getCSRFToken();
        formData['type'] = 'query';

        $.ajax({
            url: '../sop_e/?eid=92111',
            type: 'POST',
            contentType: 'application/x-www-form-urlencoded',
            data: formData,
            success: function(response) {
                layer.close(index);
                allData = response.data;
                table.reload('currentTableId', {
                    data: response.data,
                    cols: [response.cols]  // 确保这里正确设置列信息
                });
                bindToolBtnEvent();
            },

            error: function(xhr, status, error) {
                layer.close(index);
                layer.alert('请求数据失败: ' + error, {icon: 2, title: '错误信息'});
            }
        });
    }

    function bindToolBtnEvent() {
        $('.demoTable').off('click', '.layui-btn').on('click', '.layui-btn', function() {
            var type = $(this).data('type');
            if (active[type]) {
                var obj = { config: table.config };
                active[type].call(this, obj);
            }
        });
    }

    // Form提交处理
    form.on('submit(submitBtn)', function(data) {
        var formData = $('#myForm').serializeArray();
        formData.push({ name: 'eid', value: JSON.parse($('#eid').text()) });
        formData.push({ name: 'action', value: $(this).attr('value') });
        formData.push({ name: 'table', value: $('#table').val() });
        formData.push({ name: 'source', value: $('input[type=checkbox]:checked').map(function() { return $(this).val(); }).get() });
        formData.push({ name: 'view_sp', value: JSON.parse($('#view_sp').text()) });
        sendQueryRequest(JSON.stringify(formData));
        return false;
    });

});

=======
layui.use(['form', 'table', 'jquery', 'laydate', 'layer', 'util'], function() {
    var $ = layui.jquery;
    var form = layui.form;
    var table = layui.table;
    var laydate = layui.laydate;
    var layer = layui.layer;
    var util = layui.util;
    var allData = [];

    active = {
        search: function(){
            $("#search0").click();
        },
        reset: function(){
            $("#reset0").click();
            return false;
        },
        set_view_sp: function(){
            var xhr = new XMLHttpRequest();
            var eid = JSON.parse(document.getElementById('eid').textContent);
            var tb = document.getElementById('table').value;
            window.location.href = window.location.protocol + '//' + window.location.host + '/sop_e/set_view_sp/?eid=' + eid + '&tb=' + tb;
        },
        exportAll: function(obj){
            console.log(allData);
            console.log(obj)
            if (allData.length > 0) {
                    table.exportFile('currentTableId', allData, 'csv');
                } else {
                    layer.msg('没有数据可以导出');
                }
        }
        ,save: function(){
            layer.confirm('<input type="text" id="recordName" class="layui-input" style="width:600px">', {
                    title: '请输入记录名',
                    btn: ['提交', '关闭']
                }, function(index, layero){
                    var inputValue = $('#recordName').val();
                    if(inputValue === '') {
                        $('#recordName').focus();
                        return false; // 阻止对话框关闭
                    }
                    layer.msg('获得：' + inputValue);
                    layer.close(index);
                });
        }
      };

    // 初始化日期选择器
    laydate.render({
        elem: '#date',
        theme: 'molv',
        range: ['#date1', '#date2']
    });

    // 初始化表格
    table.render({
        elem: '#currentTableId',
        id: 'currentTableId',
        title: JSON.parse(document.getElementById('eid').textContent),
        height: 748,
        cols: [],
        data: [],
        page: true,
        limit: 15,
        limits: [10, 15, 20, 25, 50, 100],
        skin: 'grid',
        even: true,
        toolbar: '#demoTable',
        defaultToolbar: [],
        done: function(res, curr, count) {
            bindToolBtnEvent();
        }
    });

    function getCSRFToken() {
        var cookies = document.cookie.split(';');
        for (var i = 0; i < cookies.length; i++) {
            var cookie = cookies[i].trim();
            var cookieParts = cookie.split('=');
            if (cookieParts[0] === 'csrftoken') {
                return cookieParts[1];
            }
        }
        return null;
    }

    function sendQueryRequest(data) {
        var index = layer.load(1, {shade: [0.1, '#fff']});
        var formData = JSON.parse(data);
        formData['csrfmiddlewaretoken'] = getCSRFToken();
        formData['type'] = 'query';

        $.ajax({
            url: '../sop_e/?eid=92111',
            type: 'POST',
            contentType: 'application/x-www-form-urlencoded',
            data: formData,
            success: function(response) {
                layer.close(index);
                allData = response.data;
                table.reload('currentTableId', {
                    data: response.data,
                    cols: [response.cols]  // 确保这里正确设置列信息
                });
                bindToolBtnEvent();
            },

            error: function(xhr, status, error) {
                layer.close(index);
                layer.alert('请求数据失败: ' + error, {icon: 2, title: '错误信息'});
            }
        });
    }

    function bindToolBtnEvent() {
        $('.demoTable').off('click', '.layui-btn').on('click', '.layui-btn', function() {
            var type = $(this).data('type');
            if (active[type]) {
                var obj = { config: table.config };
                active[type].call(this, obj);
            }
        });
    }

    // Form提交处理
    form.on('submit(submitBtn)', function(data) {
        var formData = $('#myForm').serializeArray();
        formData.push({ name: 'eid', value: JSON.parse($('#eid').text()) });
        formData.push({ name: 'action', value: $(this).attr('value') });
        formData.push({ name: 'table', value: $('#table').val() });
        formData.push({ name: 'source', value: $('input[type=checkbox]:checked').map(function() { return $(this).val(); }).get() });
        formData.push({ name: 'view_sp', value: JSON.parse($('#view_sp').text()) });
        sendQueryRequest(JSON.stringify(formData));
        return false;
    });

});

>>>>>>> c7ad5a18749c6af59354deb3728da04e8b425ab0
