# -*- coding: utf-8 -*-
def create_table(fields,data,total_nums,total_sales):
    default = """layui.use('table', function(){
    var table = layui.table;
    table.render({
    elem:'#idTest',
    data:%s,
    toolbar: true, //仅开启工具栏，不显示左侧模板
    toolbar: '#demoTable',
    totalRow: true, //开启合计行
    page:true,//开启分页
    request: {
    pageName: 'page', //页码的参数名称，默认：page,
    limitName: 'limit' //每页数据量的参数名，默认：limit
    },
    height:'full-140',
    limit:10,
    limits:[10,20,30,40,50,100,200],
    cols:[[
    // 表头，对应数据格式，此示例只设置3格
    // 若不填写width列宽将自动分配
    {type: 'checkbox', totalRowText: '汇总'},\n"""% (data)
    cols = ''
    for field in fields:
        if field == '销量':
            cols += "    {" + f"field:'{field}',title:'{field}', sort: true , totalRowText: '{total_nums}'"+"},\n"
        elif field == '销售额':
            cols += "    {" + f"field:'{field}',title:'{field}', sort: true , totalRowText: '{total_sales}"+"'},\n"
        elif field == ['name','宝贝名称','标题']:
            cols += "    {" + f"field:'{field}',title:'{field}', sort: true ,"+"""templet:'<div><a href="{{d.url}}" target="_blank " title="点击查看">{{d.name}}</a></div>'},\n"""
        elif field in ['图片','img']:
            cols += "    {" + f"field:'{field}',title:'{field}', width:'5.5%',sort: true ,"+"""templet:'<div><a href="{{d.图片}}" class="layui-table-link" target="_blank" title="点击查看"><img src="{{d.图片}}" style="max-width:100%;height:60px"></a></div>'},\n"""
        elif field in ['url','row_num']:
            cols = cols
        else:
            cols += "    {" + f"field:'{field}',title:'{field}', sort: true , totalRowText: '-'"+"},\n"
    table = default + cols +"    {title:'edit',  toolbar: '#barDemo'}\n   ]],\n   });\n});"
    with open('./static/dist/layui/table.js', 'w',encoding='UTF-8') as f:
        f.write(table)
    f.close()
# -*- coding: utf-8 -*-
def new_sp(data):
    new_sp = """//准备视图对象
window.viewObj = {
    tbData: %s,
    typeData: [
        {id: 1, name: '普通属性'},
        {id: 2, name: '度量属性(聚合值)'},
    ],
    renderSelectOptions: function(data, settings){
        settings =  settings || {};
        var valueField = settings.valueField || 'value',
            textField = settings.textField || 'text',
            selectedValue = settings.selectedValue || "";
        var html = [];
        for(var i=0, item; i < data.length; i++){
            item = data[i];
            html.push('<option value="');
            html.push(item[valueField]);
            html.push('"');
            if(selectedValue && item[valueField] == selectedValue ){
                html.push(' selected="selected"');
            }
            html.push('>');
            html.push(item[textField]);
            html.push('</option>');
        }
        return html.join('');
    }
};"""% (data)

    with open('./static/dist/JS/new_sp.js', 'w',encoding='UTF-8') as f:
        f.write(new_sp)
    f.close()
