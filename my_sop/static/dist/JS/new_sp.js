//准备视图对象
window.viewObj = {
    tbData: [{'name': '品牌', 'type': 1, 'rank': 1, 'state': 0}, {'name': '子品类', 'type': 1, 'rank': 2, 'state': 0}, {'name': '是否人工答题', 'type': 1, 'rank': 3, 'state': 0}, {'name': '套包宝贝', 'type': 1, 'rank': 4, 'state': 0}, {'name': '店铺分类', 'type': 1, 'rank': 5, 'state': 0}, {'name': '疑似新品', 'type': 1, 'rank': 6, 'state': 0}, {'name': '一级类目', 'type': 1, 'rank': 7, 'state': 0}, {'name': '三级类目', 'type': 1, 'rank': 8, 'state': 0}, {'name': '二级类目', 'type': 1, 'rank': 9, 'state': 0}, {'name': 'Division', 'type': 1, 'rank': 10, 'state': 0}, {'name': 'Manufacturer', 'type': 1, 'rank': 11, 'state': 0}, {'name': 'Selectivity', 'type': 1, 'rank': 12, 'state': 0}, {'name': 'SubChannel', 'type': 1, 'rank': 13, 'state': 0}, {'name': 'TargetUser', 'type': 1, 'rank': 14, 'state': 0}, {'name': '辅助_品牌划分', 'type': 1, 'rank': 15, 'state': 0}, {'name': 'Top_flag', 'type': 1, 'rank': 16, 'state': 0}, {'name': 'District', 'type': 1, 'rank': 17, 'state': 0}, {'name': 'Region', 'type': 1, 'rank': 18, 'state': 0}],
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
};