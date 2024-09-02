layui.use('table', function(){
    var table = layui.table;
    table.render({
    elem:'#idTest',
    url:[{"\u5b50\u54c1\u7c7b":"\u5176\u5b83","\u9500\u91cf":1690696793,"\u9500\u552e\u989d":76275463085.2200012207},{"\u5b50\u54c1\u7c7b":"\u6d17\u53d1\u6c34","\u9500\u91cf":1162184799,"\u9500\u552e\u989d":75739094112.5399932861},{"\u5b50\u54c1\u7c7b":"\u5957\u88c5","\u9500\u91cf":555453775,"\u9500\u552e\u989d":37737207356.6699981689},{"\u5b50\u54c1\u7c7b":"\u5934\u53d1\u62a4\u7406","\u9500\u91cf":361290650,"\u9500\u552e\u989d":20632686125.3699989319},{"\u5b50\u54c1\u7c7b":"\u5934\u76ae\u62a4\u7406","\u9500\u91cf":43661131,"\u9500\u552e\u989d":7351138051.470000267},{"\u5b50\u54c1\u7c7b":"\u62a4\u53d1\u7d20","\u9500\u91cf":129065438,"\u9500\u552e\u989d":5704350480.9600000381},{"\u5b50\u54c1\u7c7b":"\u5e72\u6027\u6d17\u53d1","\u9500\u91cf":42798457,"\u9500\u552e\u989d":2317353961.8000001907},{"\u5b50\u54c1\u7c7b":"\u5934\u76ae\u6e05\u6d01","\u9500\u91cf":11616430,"\u9500\u552e\u989d":1075924198.9900000095}],
    toolbar: true, //仅开启工具栏，不显示左侧模板
    toolbar: '#demoTable',
    totalRow: false, //开启合计行
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
    {type: 'checkbox', totalRowText: '汇总'},
    {field:'子品类',title:'子品类', sort: true , totalRowText: '-'},
    {field:'销量',title:'销量', sort: true , totalRowText: '3996767473'},
    {field:'销售额',title:'销售额', sort: true , totalRowText: '226833217373.02'},
    {title:'edit',  toolbar: '#barDemo'}
   ]],
   });
});