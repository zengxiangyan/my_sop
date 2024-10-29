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

// 初始化 CodeMirror
var sqlEditor = CodeMirror(document.getElementById('editor'), {
    mode: 'text/x-sql',
    theme: 'default',
    lineNumbers: true,
    indentUnit: 2,
    autofocus: true,
    viewportMargin: Infinity
});


function resizeEditor() {
    // 获取当前编辑器的高度
    var ruleContainer = document.getElementById('rule');
    var editorContainer = document.getElementById('editorContainer');
    var sqlreviewContainer = document.getElementById('sqlreview');
    var toolContainer = document.getElementById('tool');
    var dividerContainer = document.getElementById('divider');
    var tipContainer = document.getElementById('tip');
    var currentEditorHeight = sqlEditor.getWrapperElement().offsetHeight;

    // 设置编辑器的高度
    sqlEditor.setSize(null, 'auto');
    toolContainer.style.marginTop = '30px'; // 20为额外的间隔
    editorContainer.style.height = (currentEditorHeight + 250) + 'px';
//    toolContainer.style.marginTop = (currentEditorHeight - sqlreviewheight0 + 60 ) + 'px'; // 20为额外的间隔
    dividerContainer.style.height = (editorContainer.offsetHeight + sqlreviewContainer.offsetHeight) + 15 + 'px';
    tipContainer.style.height = (editorContainer.offsetHeight + sqlreviewContainer.offsetHeight) + 15 + 'px';
    ruleContainer.style.height = (editorContainer.offsetHeight + sqlreviewContainer.offsetHeight) + 15 + 'px';
}

resizeEditor();

function imgHover() {
        $('.img-wrap').hover(function(){
            $('.img-wrap').removeClass('active')
            $(this).children('img').addClass('active')
        },function(){
            $(this).children('img').removeClass('active')
        })
    }
    function show_all_fields(){
        $('#all').toggle();
    }

function show(res,type){
    $("#now").val('');
    $("#history").val('');
    $('#load').empty();
//    修改type
    $("#type").attr("value",type);
    var str='';
    console.log(res.head);

    if(["all"].indexOf(type)>-1){
        // 只显示
        var columns=[];
        for(let i=0;i<res.head.length;i++)
        {
            if(res.head[i]=="名称")
            {
                col= {
                    field: res.head[i], title: res.head[i],
                    formatter: function (value, row, index) {
                        return '<a href="'+row['链接']+'" target="_blank">'+value+'</a>';
                    }
                }
                columns.push(col);
            }
            else if(res.head[i]=="图片"){
                col= {
                    field: res.head[i], title: res.head[i],
                    formatter: function (value, row, index) {
                        return '<div class="img-wrap"><img class="img-hover" src="' + row['图片'] + '" height="30" name="item_small_img" alt="" /></div>';
                    }
                }
                columns.push(col);
            }
            else if(res.head[i]=="图片(for download)"){
                col= {
                    field: res.head[i], title: res.head[i],
                    formatter: function (value, row, index) {
                        return '<div style="display: none;">' + row['图片(for download)'] + '</div>';
                    }
                }
                columns.push(col);
            }
            else if(res.head[i]=="店铺"){
                col= {
                    field: res.head[i], title: res.head[i],
                    formatter: function (value, row, index) {
                        return '<a href="'+row['店铺链接']+'" target="_blank">'+value+'</a>';
                    }
                }
                columns.push(col);
            }
            else if(res.head[i]=="销售额" || res.head[i]=="去年同期销售额" || res.head[i]=="上月同期销售额"|| res.head[i]=="均价"||res.head[i]=="划线价销售额"||res.head[i]=="MAT LY销售额"||res.head[i]=="MAT TY销售额"||res.head[i]=="YTD LY销售额"||res.head[i]=="YTD TY销售额")
            {
                col= {
                    field: res.head[i], title: res.head[i],
                    formatter: function (value, row, index) {
                        return parseFloat(value).toFixed(2).replace(/(\d)(?=(\d{3})+\.)/g, '$1,');
                    },
                    align:'right',
                    sortable:true
                }
                columns.push(col);
            }
            else if(res.head[i]=="销售额同比"||res.head[i]=="销量同比"||res.head[i]=="销售额环比"||res.head[i]=="销量环比"){
                col= {
                    field: res.head[i], title: res.head[i],
                    formatter: function (value, row, index) {
                        if(value=='-'){
                            return '-';
                        }
                        else{
                            return parseFloat(value*100).toFixed(2)+'%';
                        }
                    },
                    align:'right',
                    sortable:true
                }
                columns.push(col);
            }
            else if(res.head[i]=="时间")
            {
                col={field:res.head[i],title:res.head[i],sortable:true};
                columns.push(col);
            }
            else if(res.head[i]!="链接" && res.head[i]!="店铺链接")
            {
                col={field:res.head[i],title:res.head[i]};
                columns.push(col);
            }
            str+=res.head[i]+',';
        }
        str+='\n';
        var data=[];
        for(i=0;i<res.content.length;i++)
        {
            var f=0;
            var col={};
            for(let j in res.content[i])
            {
                col[res.head[f]]=res.content[i][j];
                f=f+1;
                str+='"'+String(res.content[i][j]).replace(/\n/g,"").replace(/"/g,'""')+ '",';
            }
            data.push(col);
            str+='\n';
        }
        //添加下载按钮并传入值
        $("#download").show();
        $('#save').show();
        $('#update_price').show();
        $('#sampling').show();
        $("#download").val(str);
        // console.log(str);
        $('#showtable').bootstrapTable('destroy'); // 销毁现有表格
        $('#showtable').bootstrapTable({
            columns: columns,
            data: data,
            search: false,
            pagination: true,
            pageSize: 10,
            pageList: [10,30,50,100],
            onPostBody: function(data) {
                imgHover();
            }
        });
    }
    else{
        // 显示并可以添加条件
        var columns=[];
        for(let i=0;i<res.head.length;i++)
        {
            if(res.head[i]=="名称")
            {
                col= {
                    field: res.head[i], title: res.head[i],
                    formatter: function (value, row, index) {
                        return '<a href="'+row['链接']+'" target="_blank">'+value+'</a>';
                    }
                };
                columns.push(col);
            }
            else if(res.head[i]=="图片"){
                col= {
                    field: res.head[i], title: res.head[i],
                    formatter: function (value, row, index) {
                        return '<div class="img-wrap"><img class="img-hover" src="' + row['图片'] + '" height="30" name="item_small_img" alt="" /></div>';
                    }
                }
                columns.push(col);
            }
            else if(res.head[i]=="图片(for download)"){
                col= {
                    field: res.head[i], title: res.head[i],
                    formatter: function (value, row, index) {
                        return '<div style="display: none;">' + row['图片(for download)'] + '</div>';
                    }
                }
                columns.push(col);
            }
            else if(res.head[i]=="店铺"){
                col= {
                    field: res.head[i], title: res.head[i],
                    formatter: function (value, row, index) {
                        return '<a href="'+row['店铺链接']+'" target="_blank">'+value+'</a>';
                    }
                };
                columns.push(col);
            }
            else if(res.head[i]=="销售额" || res.head[i]=="去年同期销售额" || res.head[i]=="上月同期销售额"|| res.head[i]=="均价"||res.head[i]=="划线价销售额"||res.head[i]=="MAT LY销售额"||res.head[i]=="MAT TY销售额"||res.head[i]=="YTD LY销售额"||res.head[i]=="YTD TY销售额")
            {
                col= {
                    field: res.head[i], title: res.head[i],
                    formatter: function (value, row, index) {
                        return parseFloat(value).toFixed(2).replace(/(\d)(?=(\d{3})+\.)/g, '$1,');
                    },
                    align:'right',
                    sortable:true
                }
                columns.push(col);
            }
            else if(res.head[i]=="销售额占比" || res.head[i]=="销量占比"||res.head[i]=="销售额同比"||res.head[i]=="销量同比"||res.head[i]=="销售额环比"||res.head[i]=="销量环比"){
                col= {
                    field: res.head[i], title: res.head[i],
                    formatter: function (value, row, index) {
                        if (value == '-') {
                            return '-';
                        }
                        else {
                            return parseFloat(value * 100).toFixed(2) + '%';
                        }
                    },
                    align:'right',
                    sortable:true
                };
                columns.push(col);
            }
            else if(res.head[i]=="时间")
            {
                col={field:res.head[i],title:res.head[i],sortable:true};
                columns.push(col);
            }
            else if(res.head[i]!="链接" && res.head[i]!="店铺链接")
            {
                col={field:res.head[i],title:res.head[i]};
                columns.push(col);
            }
            str+=res.head[i]+',';
        }
        str+='\n';
        columns.push({checkbox:true});
        // columns.push({field: 'operate',title: '操作',formatter:orerateFormatter});
        var data=[];
        for(i=0;i<res.content.length;i++)
        {
            var f=0;
            var col={};
            for(let j in res.content[i])
            {
                col[res.head[f]]=res.content[i][j];
                f=f+1;
                str+='"'+String(res.content[i][j]).replace(/\n/g,"").replace('"','""')+ '",';
            }
            str+='\n';
            data.push(col);
        }
        //添加下载按钮并传入值
        $("#download").show();
        $("#save").show();
        $('#update_price').show();
        $("#sampling").show();
        $("#download").val(str);
        $('#showtable').bootstrapTable('destroy'); // 销毁现有表格
        $('#showtable').bootstrapTable({
            columns:columns,
            data:data,
            search: false,
            pagination: true,
            pageSize: 10,
            pageList: [10,30,50,100],
            onPostBody:function(data){
                imgHover()
            }
        })
    }
}

function down() {
    str = $('#download').val();

    var now = new Date();
    var timestamp = now.getFullYear().toString() +
                    ('0' + (now.getMonth() + 1)).slice(-2) + // 月份从 0 开始
                    ('0' + now.getDate()).slice(-2) +
                    ('0' + now.getHours()).slice(-2) +
                    ('0' + now.getMinutes()).slice(-2) +
                    ('0' + now.getSeconds()).slice(-2);

    let uri = 'data:text/csv;charset=utf-8,\ufeff' + encodeURIComponent(str);
    var link = document.createElement("a");
    link.href = uri;
    link.download = timestamp + ".csv";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}


function refresh() {
    $.ajax({
        type: 'post',
        url: '../query_list',
        headers: {'X-CSRFToken': getCSRFToken()},
        success: function(response) {
            const queries = response.queries;
            const tipContainer = $('#tip .layui-card-body');
            tipContainer.empty();

            if (queries.length > 0) {
                queries.forEach(query => {
                    const noticeHtml = `
                        <div class="layuimini-notice">
                            <div class="layuimini-notice-title">${query.title}</div>
                            <div class="layuimini-notice-extra">${query.update_time}</div>
                            <div class="layuimini-notice-content layui-hide">
                                ${query.comment}
                            </div>
                        </div>
                    `;
                    tipContainer.append(noticeHtml);
                });
            } else {
                const emptyNotice = `
                    <div class="layuimini-notice">
                        <div class="layuimini-notice-title">你还没有保存过！！！</div>
                    </div>
                `;
                tipContainer.append(emptyNotice);
            }
        },
        error: function(xhr, status, error) {
            console.error("获取历史记录失败：", error);
        }
    });
}


function commit(){
    if($('#record_name').val()==''){
        alert('记录名不能为空');
        return;
    }
    var Ai_help = document.getElementById("Ai_help").innerHTML;
    var db=document.getElementById('db').value;
    var user=document.getElementById('user').value;
    var recordname=$('#record_name').val();
    var comment=$('#comments').val();
    var sqlContent = sqlEditor.getValue();
    console.log({ user: user,sql_query:sqlContent,Ai_help:Ai_help, db: db, title: recordname,comment:comment });
    $.ajax({
        type: 'post',
        url: '../updaterecord',
        data: { user: user,sql_query:sqlContent,Ai_help:Ai_help, db: db, title: recordname,comment:comment },
        success: function (res) {
            try {
                if (res.code == 200 || res.code == 0) {
                    $('#recordname').modal('hide');
                    alert(res.message);
                    refresh();
                    return;
                }
                alert('名称不能重复');
            } catch (e) {
                console.error('解析响应失败:', e);
                alert('服务器返回数据格式错误，请稍后重试。');
            }
        },
        error: function (xhr, status, error) {
            console.error('请求失败:', error);
            alert('名称不能重复');
        }
    });

}

layui.use(['jquery', 'laydate', 'form', 'layer'], function () {
    var $ = layui.jquery;
    var laydate = layui.laydate;
    var form = layui.form;
    var layer = layui.layer;
    var util = layui.util;

    var allData = []; // 用于存储从服务器获取的所有数据

    resizeEditor();

    sqlEditor.on('change', resizeEditor);
    window.addEventListener('resize', resizeEditor);
    sqlEditor.on('delete', resizeEditor);
    sqlEditor.on('paste', resizeEditor);

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

    form.on('submit(data-search-btn)', function (data) {
        var sqlContent = sqlEditor.getValue();
        var index = layer.load(1, {shade: [0.1, '#fff']});
        var db = document.getElementById("db").value;
        var Ai_help = document.getElementById("Ai_help").innerHTML;
        $('#showtable').bootstrapTable('destroy');
        $.ajax({
            url: '../sql_search/',
            type: 'POST',
            contentType: 'application/json',
            data: JSON.stringify({'db':db,'sql': sqlContent,'ai_help':Ai_help}),
            headers: {'X-CSRFToken': getCSRFToken()},
            success: function(res) {
                layer.close(index);

                if (res.code === 200) {
                    allData = res.data; // 保存完整数据
                    show(res.data,res.toptype);
                } else {
                    console.log(res);
                    console.log(res.msg);
                    layer.alert('请求数据失败: ' + res.msg, {icon: 2, title: '错误信息'});
                }

            },
            error: function(xhr, status, error) {
                layer.close(index);
                layer.alert('请求数据失败: ' + error, {icon: 2, title: '错误信息'});
            }
        });

        return false; // 阻止表单默认提交
    });
    $('body').on('click', '.layuimini-notice', function () {
        var title = $(this).children('.layuimini-notice-title').text(),
            noticeTime = $(this).children('.layuimini-notice-extra').text(),
            db_this = $(this).find('#dbquery').val(),
            content = $(this).children('.layuimini-notice-content').html();
        sql_query = sql_query.replace(/\\n/g, '\n');
        
        var sql_query = sql_query_elem.length ? sql_query_elem.val() : '';
        console.log(sql_query);
        var html = '<div style="padding:15px 20px; text-align:justify; line-height: 22px;border-bottom:1px solid #e2e2e2;background-color: #e2e2e2;color: #73879C">\n' +
            '<div style="text-align: center;margin-bottom: 20px;font-weight: bold;border-bottom:1px solid #718fb5;padding-bottom: 5px"><h4 class="text-danger">' + title + '</h4></div>\n' +
            '<div style="font-size: 12px">' + content + '</div>\n' +
            '</div>\n';
        parent.layer.open({
            type: 1,
            title: 'QueryDetail'+'<span style="float: right;right: 1px;font-size: 12px;color: #b1b3b9;margin-top: 1px">'+noticeTime+'</span>',
            area: '300px;',
            shade: 0.8,
            id: 'layuimini-notice',
            btn: ['Query', 'Close'],
            btnAlign: 'c',
            moveType: 1,
            content:html,
            success: function (layero) {
                var btn = layero.find('.layui-layer-btn');

                btn.find('.layui-layer-btn0').on('click', function() {

                    sqlEditor.setValue('');
                    $('#db').val(db_this);
                    form.render('select');
                    console.log(db_this);
                    sqlEditor.setValue(sql_query);
                    resizeEditor();
                    // 触发 "Run" 按钮的点击事件
                    $('#Run-sqledit').click();
                });
            }

        });
    });
});

document.getElementById("toggleSwitch").addEventListener("click", function() {
    var Ai_help = document.getElementById("Ai_help");

    if (Ai_help.innerHTML === "是") {
        Ai_help.innerHTML = "否";
        this.classList.remove("layui-form-onswitch");
        this.classList.add("layui-form-offswitch");
    } else {
        Ai_help.innerHTML = "是";
        this.classList.remove("layui-form-offswitch");
        this.classList.add("layui-form-onswitch");
    }
});


