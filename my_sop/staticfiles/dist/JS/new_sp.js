function fetchDataSync() {
    try {
        const eid = JSON.parse(document.getElementById('eid').textContent);
        const tb = JSON.parse(document.getElementById('tb').textContent);

        var xhr = new XMLHttpRequest();
        xhr.open('GET', `../get_view_sp/?eid=${eid}&tb=${tb}`, false);
        xhr.send(null);

        if (xhr.status === 200) {
            const data = JSON.parse(xhr.responseText);
            if (data && data.data) {
                window.viewObj.tbData = data.data;
            } else {
                throw new Error('No data returned from the server');
            }
        } else {
            throw new Error('Server responded with status: ' + xhr.status);
        }
    } catch (error) {
        console.error('Error fetching tbData:', error);
    }
};

window.viewObj = {
    tbData: [], // 初始为空数组
    typeData: [
        {id: 1, name: '普通属性'},
        {id: 2, name: '度量属性(聚合值)'},
    ],

    renderSelectOptions: function(data, settings) {
        settings = settings || {};
        const valueField = settings.valueField || 'value';
        const textField = settings.textField || 'text';
        const selectedValue = settings.selectedValue || "";
        return data.map(item =>
            `<option value="${item[valueField]}"${selectedValue && item[valueField] == selectedValue ? ' selected="selected"' : ''}>${item[textField]}</option>`
        ).join('');
    }
};

fetchDataSync();
