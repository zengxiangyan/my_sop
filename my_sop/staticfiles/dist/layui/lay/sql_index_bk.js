<<<<<<< HEAD
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
};

layui.use(['jquery', 'table', 'laydate', 'form', 'layer'], function () {
    var $ = layui.jquery;
    var table = layui.table;
    var laydate = layui.laydate;
    var form = layui.form;
    var layer = layui.layer;

    var allData = []; // 用于存储从服务器获取的所有数据

    // 初始化 CodeMirror
    var sqlEditor = CodeMirror(document.getElementById('editor'), {
        mode: 'text/x-sql',
        theme: 'default',
        lineNumbers: true,
        indentUnit: 2,
        autofocus: true,
        viewportMargin: Infinity
    });

    var editorContainer = document.getElementById('editorContainer');
    function resizeEditor() {
        var currentEditorHeight = sqlEditor.getWrapperElement().offsetHeight;
        sqlEditor.setSize(null, 'auto');
        editorContainer.style.height = currentEditorHeight + 'px';
    }

    resizeEditor();
    sqlEditor.on('change', resizeEditor);
    window.addEventListener('resize', resizeEditor);

    var copyButton = document.getElementById('copy-sqledit');
    copyButton.addEventListener('click', function(event) {
        var sqlContent = sqlEditor.getValue();
        if (navigator.clipboard) {
            navigator.clipboard.writeText(sqlContent).then(function() {
                layer.msg('SQL 语句已复制到剪贴板');
            }, function(err) {
                layer.msg('复制失败');
            });
        } else {
            // Fallback using execCommand
            var textarea = document.createElement('textarea');
            textarea.value = sqlContent;
            document.body.appendChild(textarea);
            textarea.select();
            try {
                var successful = document.execCommand('copy');
                var msg = successful ? '成功复制' : '复制失败';
                layer.msg('SQL 语句已复制到剪贴板: ' + msg);
            } catch (err) {
                layer.msg('复制失败');
            }
            document.body.removeChild(textarea);
        }
    });

    table.render({
        elem: '#currentTableId',
        title: 'table',
        cols: [],
        data: [],
        page: true,
        limit: 15,
        limits: [10, 15, 20, 25, 50, 100],
        skin: 'line',
        toolbar: '#toolbarDemo',
        defaultToolbar: ['filter', 'exports', 'print', {
            title: '提示',
            layEvent: 'LAYTABLE_TIPS',
            icon: 'layui-icon-tips'
        }]
    });

    // 头工具栏事件
    table.on('tool(demo)', function(obj){
        switch(obj.event){
            case 'exportAll':
                // 导出所有数据
                if (allData.length > 0) {
                    table.exportFile(obj.config.id, allData, 'csv');
                } else {
                    layer.msg('没有数据可以导出');
                }
                break;
            case 'print':
                console.log(123)
                // 处理打印逻辑
                break;
        }
    });

    form.on('submit(data-search-btn)', function (data) {
        var sqlContent = sqlEditor.getValue();
        var index = layer.load(1, {shade: [0.1, '#fff']});

        $.ajax({
            url: '../sql_search/',
            type: 'POST',
            contentType: 'application/json',
            data: JSON.stringify({'sql': sqlContent}),
            headers: {'X-CSRFToken': getCSRFToken()},
            success: function(response) {
                layer.close(index);
                allData = response.data; // 保存完整数据
                table.reload('currentTableId', {
                    data: response.data,
                    cols: [response.cols]
                });
            },
            error: function(xhr, status, error) {
                layer.close(index);
                layer.alert('请求数据失败: ' + error, {icon: 2, title: '错误信息'});
            }
        });

        return false; // 阻止表单默认提交
    });
=======
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
};

layui.use(['jquery', 'table', 'laydate', 'form', 'layer'], function () {
    var $ = layui.jquery;
    var table = layui.table;
    var laydate = layui.laydate;
    var form = layui.form;
    var layer = layui.layer;

    var allData = []; // 用于存储从服务器获取的所有数据

    // 初始化 CodeMirror
    var sqlEditor = CodeMirror(document.getElementById('editor'), {
        mode: 'text/x-sql',
        theme: 'default',
        lineNumbers: true,
        indentUnit: 2,
        autofocus: true,
        viewportMargin: Infinity
    });

    var editorContainer = document.getElementById('editorContainer');
    function resizeEditor() {
        var currentEditorHeight = sqlEditor.getWrapperElement().offsetHeight;
        sqlEditor.setSize(null, 'auto');
        editorContainer.style.height = currentEditorHeight + 'px';
    }

    resizeEditor();
    sqlEditor.on('change', resizeEditor);
    window.addEventListener('resize', resizeEditor);

    var copyButton = document.getElementById('copy-sqledit');
    copyButton.addEventListener('click', function(event) {
        var sqlContent = sqlEditor.getValue();
        if (navigator.clipboard) {
            navigator.clipboard.writeText(sqlContent).then(function() {
                layer.msg('SQL 语句已复制到剪贴板');
            }, function(err) {
                layer.msg('复制失败');
            });
        } else {
            // Fallback using execCommand
            var textarea = document.createElement('textarea');
            textarea.value = sqlContent;
            document.body.appendChild(textarea);
            textarea.select();
            try {
                var successful = document.execCommand('copy');
                var msg = successful ? '成功复制' : '复制失败';
                layer.msg('SQL 语句已复制到剪贴板: ' + msg);
            } catch (err) {
                layer.msg('复制失败');
            }
            document.body.removeChild(textarea);
        }
    });

    table.render({
        elem: '#currentTableId',
        title: 'table',
        cols: [],
        data: [],
        page: true,
        limit: 15,
        limits: [10, 15, 20, 25, 50, 100],
        skin: 'line',
        toolbar: '#toolbarDemo',
        defaultToolbar: ['filter', 'exports', 'print', {
            title: '提示',
            layEvent: 'LAYTABLE_TIPS',
            icon: 'layui-icon-tips'
        }]
    });

    // 头工具栏事件
    table.on('tool(demo)', function(obj){
        switch(obj.event){
            case 'exportAll':
                // 导出所有数据
                if (allData.length > 0) {
                    table.exportFile(obj.config.id, allData, 'csv');
                } else {
                    layer.msg('没有数据可以导出');
                }
                break;
            case 'print':
                console.log(123)
                // 处理打印逻辑
                break;
        }
    });

    form.on('submit(data-search-btn)', function (data) {
        var sqlContent = sqlEditor.getValue();
        var index = layer.load(1, {shade: [0.1, '#fff']});

        $.ajax({
            url: '../sql_search/',
            type: 'POST',
            contentType: 'application/json',
            data: JSON.stringify({'sql': sqlContent}),
            headers: {'X-CSRFToken': getCSRFToken()},
            success: function(response) {
                layer.close(index);
                allData = response.data; // 保存完整数据
                table.reload('currentTableId', {
                    data: response.data,
                    cols: [response.cols]
                });
            },
            error: function(xhr, status, error) {
                layer.close(index);
                layer.alert('请求数据失败: ' + error, {icon: 2, title: '错误信息'});
            }
        });

        return false; // 阻止表单默认提交
    });
>>>>>>> c7ad5a18749c6af59354deb3728da04e8b425ab0
});