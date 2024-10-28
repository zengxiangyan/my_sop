// excel
class Excel {
    constructor(dom, cols, cfg={}) {
        this.dom = dom
        this.sheet = null
        this.warp = false
        this.cols = cols
        this.data = {}
        this.deleted = {}
        this.chgcfg = cfg

        this.pkey = -1 // 没pkey的不能编辑
        this.readonly = []
        cols.forEach((c, i) => {
            if (c.hasOwnProperty('pk')) {
                this.pkey = i
            }
            if (c.hasOwnProperty('readonly')) {
                this.readonly.push(i)
            }
        });

        this.tinydom = undefined
        this.tinysheet = undefined
        this.tinypoint = undefined
        this.tinydata = undefined
    }

    toggle_warp(s=true) {
        if (s) {
            this.warp = !this.warp
        }
        this.dom.find('td').css('white-space', this.warp ? 'pre-wrap' : '')
    }

    set_data(data) {
        if (this.pkey > -1) {
            this.data = {}
            data.forEach(v => {
                this.data[v[this.pkey]] = v
            });
        }
        data = JSON.parse(JSON.stringify(data))
        this.sheet.setData(data)
        this.toggle_warp(false)
    }

    get_lastid() {
        let id = ''
        if (this.pkey > -1) {
            id = 0
            for (const k in this.data) {
                id = Math.max(k, id)
            }
            id+= 1
        }
        return id
    }

    open_jsonedit(col, row, v) {
        let cols = [{ title: 'key' }, { title: 'val' }]
        let data = []
        for (const k in v) {
            data.push([k, v[k]])
        }
        if (data.length == 0) {
            data.push(['', '']) // 存个空的 不然页面上空行都没有
        }
        this.open_tinysheet(col, row, cols, data)
    }

    open_tinysheet(col, row, cols, data) {
        let dom = "<div id='shadow' style='width: 100%;height: 100%;position: fixed;z-index: 999;left: 0;top: 0;text-align: center;'>"+
                        "<div style='width: 100%;height: 100%;background-color: gray;opacity: 0.6;position: fixed;'></div>"+
                        "<div id='spreadsheet_tiny' style='margin-top:100px'></div>"+
                        "<div style='top: 100px;left: 40%;position: fixed;'>"+
                            "<a class='btn btn-info' style='margin-right: 20px;'>Cancel</a>"+
                            "<a class='btn btn-success'>Save</a>"+
                        "</div>"+
                    "</div>"
        this.tinydom = $(dom)
        $("body").append(this.tinydom);

        let root = this
        this.tinydata = data
        this.tinypoint = [col, row]
        this.tinysheet = jexcel(document.getElementById('spreadsheet_tiny'), {
            data: JSON.parse(JSON.stringify(data)),
            columns: cols,
            search: true,
            tableOverflow: true,
            tableWidth: (document.body.clientWidth - 300) + 'px',
            tableHeight: (document.body.clientHeight - 100) + 'px',
            allowInsertColumn: false,
            allowDeleteColumn: false,
            defaultColWidth: 300,
            updateTable: function (obj, cell, col, row, val) {
                if (row + 1 > root.tinydata.length || val != root.tinydata[row][col]) {
                    cell.style.backgroundColor = '#FFE4E1';
                } else {
                    cell.style.backgroundColor = '';
                }
            },
            onbeforechange: function (obj, cell, col, row, val) {
                return typeof val == 'string' ? val.trim() : val
            }
        })
        // this.editsheet.hideIndex()

        $('#shadow .btn-success').click(function () {
            root.close_edit(true)
        })

        $('#shadow .btn-info').click(function () {
            root.close_edit(false)
        })
    }

    close_edit(save=false) {
        if (save) {
            let d = this.tinysheet.getData()
            let data = this.sheet.getRowData(this.tinypoint[1])
            let tmp = {}
            d.forEach(v => {
                if (v[0] != '') {
                    tmp[v[0]] = v[1]
                }
            });
            data[this.tinypoint[0]] = JSON.stringify(tmp)

            this.sheet.setRowData(this.tinypoint[1], data)
        }
        this.tinysheet.destroy()
        this.tinydom.remove()
        this.tinysheet = undefined
        this.tinypoint = undefined
        this.tinydata = undefined
        this.tinydom = undefined
    }

    get_changed_val() {
        if (this.pkey == -1) {
            return []
        }
        let ret = []
        let data = this.sheet.getData()
        data.forEach(v => {
            if (!this.data.hasOwnProperty(v[this.pkey])) {
                let d = { '__opr': 'add' }
                this.sheet.getConfig().columns.forEach(function (c, i) {
                    d[c['name'] || c['title']] = v[i]
                });
                ret.push(d)
            } else if (this.deleted.hasOwnProperty(v[this.pkey])) {
                let d = { '__opr': 'del', '__pkey': v[this.pkey] }
                ret.push(d)
            } else {
                let d = {}
                let self = this
                this.sheet.getConfig().columns.forEach(function (c, i) {
                    if (c.hasOwnProperty('readonly')) {
                        return
                    }
                    if (v[i] != self.data[v[self.pkey]][i]) {
                        d[c['name'] || c['title']] = v[i]
                    }
                });
                if (Object.keys(d).length > 0) {
                    d['__opr'] = 'modify'
                    d['__pkey'] = v[this.pkey]
                    ret.push(d)
                }
            }
        });
        return ret
    }

    render() {
        let root = this
        let event = function (e, obj, cell, col, row) {
            if (root.sheet == undefined || root.pkey == -1) {
                return
            }

            let colcfg = root.sheet.getConfig()['columns']
            if (e == 'oneditionstart' && colcfg[col].hasOwnProperty('format')) {
                let type = colcfg[col]['format']
                let vv = root.sheet.getRowData(row)[col]
                if (type == 'json') {
                    try {
                        vv = JSON.parse(vv)
                    } catch (error) {
                        console.error(error)
                        vv = {'':''}
                    }
                    root.open_jsonedit(col, row, vv)
                }
            }
        }

        let change = function (obj, cell, col, row, val) {
            if (root.sheet == undefined || root.pkey == -1) {
                return typeof val == 'string' ? val.trim() : val
            }

            if (col == root.pkey) {
                let pk = root.sheet.getRowData(row)[root.pkey]
                if (root.data.hasOwnProperty(pk) && pk != val) {
                    window.alert('not allow modify pkey')
                    return pk
                }
                if (pk != val && root.data.hasOwnProperty(val)) {
                    window.alert('pkey exists')
                    return pk
                }
            }

            if (root.sheet.getConfig()['columns'][col].hasOwnProperty('readonly')) {
                return root.sheet.getRowData(row)[col]
            }

            return typeof val == 'string' ? val.trim() : val
        }

        let handler = function (obj, cell, col, row, val) {
            if (root.sheet == undefined || root.pkey == -1) {
                return
            }

            if (root.readonly.indexOf(col) > -1) {
                cell.classList.add('readonly')
            }

            let pk = root.sheet.getRowData(row)[root.pkey]

            if (root.deleted.hasOwnProperty(pk)) {
                cell.style.backgroundColor = '#CFCFCF';
                return
            }
            if (!root.data.hasOwnProperty(pk)) {
                cell.style.backgroundColor = '#FEFDCF';
                return
            }
            if (root.data[pk][col] != null || val != '')
                if (root.data[pk][col] != val) {
                    cell.style.backgroundColor = '#FFE4E1';
                    return
                }
            cell.style.backgroundColor = '';
        };

        let deleterow = function (obj, row, val) {
            if (root.sheet == undefined || root.pkey == -1) {
                return true
            }

            let pk = root.sheet.getRowData(row)[root.pkey]

            if (!root.data.hasOwnProperty(pk)) {
                return true
            }

            if (!root.deleted.hasOwnProperty(pk)) {
                root.deleted[pk] = true
            } else {
                delete root.deleted[pk]
            }

            root.sheet.setRowData(row, root.sheet.getRowData(row))
            return false
        }

        let cfg = {
            worksheetName: '',
            data: [[]],
            columns: this.cols,
            search: false,
            filters: true,
            editable: this.pkey>-1,
            tableOverflow: true,
            defaultColAlign: 'left',
            tableWidth: (document.body.clientWidth - 300) + 'px',
            tableHeight: (document.body.clientHeight - 100) + 'px',
            allowInsertColumn: false,
            allowDeleteColumn: false,
            allowRenameColumn: false,
            defaultColWidth: 300,
            onevent: event,
            onbeforechange: change,
            onbeforedeleterow: deleterow,
            updateTable: handler,
            license: '39130-64ebc-bd98e-26bc4xx'
        }
        for (const k in this.chgcfg) {
            cfg[k] = this.chgcfg[k]
        }

        this.sheet = jexcel(this.dom[0], cfg);
        this.dom.append("<a class='btn btn-info toggle_warp' )' style='margin-right: 20px;'>切换cell显示</a>")
        this.dom.find('.toggle_warp').click(function () {
            root.toggle_warp()
        })
    }
}

function dropdown_click(self) {
    dom = $(self).parents('.input-group-btn').find('.btn')[0]
    $(dom).val($(self).find('a').text()).html($(self).find('a').text())
}

function dropdown_click2(self) {
    val = $(self).find('a').attr('value') || $(self).find('a').text()
    dom = $(self).parents('.input-group-btn').find('.btn')[0]
    $(dom).val(val).html(val)
}

function delparent(self) {
    $(self).parent().remove()
}

// api
window.token = undefined;

function route(id, host, data, callback) {
    if (window.token == undefined) {
        window.alert('需要先配置this.token才能用api');
        return
    }
    $('body').append('<div class="loading"><span class="spinner-border"></span> Loading...</div>')
    $('.btn').attr('disabled', 'disabled');
    $.ajax({
        url: "../",
        type: "POST",
        async: true,
        dataType: "json",
        data: {
            id: id,
            host: host,
            data: JSON.stringify(data),
            _csrf: token,
        },
        success: function (result) {
            $('.btn').removeAttr('disabled');
            $('.loading').remove()
            let code = result.code;
            if (code != 0) {
                let errmsg = result.data;
                window.alert(errmsg);
                return;
            }
            callback(result.data)
        },
        error: function (jqXHR, textStatus, errorThrown) {
            $('.btn').removeAttr('disabled');
            $('.loading').remove()
            if (textStatus == 'parsererror') {
                $('body').append('<div style="position: absolute; width: 100%; height: 100%; z-index: 9999; top: 0px; background-color: white;">' + jqXHR.responseText + '</div>');
                // window.alert(jqXHR.responseText);
            } else {
                window.alert("处理时发生错误: " + textStatus + "\n" + jqXHR.status + " " + errorThrown);
            }
            console.log("Failed");
        },
    });
}
