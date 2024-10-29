	layui.use(['jquery', 'table', 'layer'], function(){
		var $ = layui.$, table = layui.table, form = layui.form, layer = layui.layer;

		//数据表格实例化
		var tbWidth = $("#tableRes").width();
		var layTableId = "layTable";
		var tableIns = table.render({
			elem: '#dataTable',
			id: layTableId,
			data: viewObj.tbData,
			width: tbWidth,
<!--			page: true,-->
            limit:10000,
			loading: true,
			even: false, //不开启隔行背景
			cols: [[
				{title: 'spid', type: 'numbers'},
				{field: 'name', title: 'name', edit: 'text'},
				{field: 'type', title: 'operation', templet: function(d){
					var options = viewObj.renderSelectOptions(viewObj.typeData, {valueField: "id", textField: "name", selectedValue: d.type});
					return '<a lay-event="type"></a><select name="type" lay-filter="type"><option value="">请选择分类</option>' + options + '</select>';
				}},
				{field: 'state', title: '是否启用筛选', event: 'state', templet: function(d){
					var html = ['<input type="checkbox" name="state" lay-skin="switch" lay-text="是|否"'];
					html.push(d.state > 0 ? ' checked' : '');
					html.push('>');
					return html.join('');
				}},
				{field: 'tempId', title: '操作', templet: function(d){
					return '<a class="layui-btn layui-btn-xs" lay-skin="switch" lay-event="up" lay-id="'+ d.tempId +'"><i class="layui-icon layui-icon-up"></i>上移</a>'
					+'<a class="layui-btn layui-btn-xs" lay-skin="switch" lay-event="down" lay-id="'+ d.tempId +'"><i class="layui-icon layui-icon-down"></i>下移</a>'
					+'<a class="layui-btn layui-btn-xs" lay-skin="switch" lay-event="view" lay-id="'+ d.tempId +'"><i class="layui-icon layui-icon-search"></i>view</a>';

				}}
			]],
			done: function(res, curr, count){
				viewObj.tbData = res.data;
			}
		});

		//定义事件集合
		var active = {
			addRow: function(){	//添加一行
				var oldData = table.cache[layTableId];
				console.log(oldData);
				var newRow = {tempId: new Date().valueOf(), type: null, name: '请填写名称', state: 0};
				oldData.push(newRow);
				tableIns.reload({
					data : oldData
				});
			},
			updateRow: function(obj){
				var oldData = table.cache[layTableId];
				for(var i=0, row; i < oldData.length; i++){
					row = oldData[i];
					if(row.tempId == obj.tempId){
						$.extend(oldData[i], obj);
						return;
					}
				}
				tableIns.reload({
					data : oldData
				});
			},
			removeEmptyTableCache: function(){
				var oldData = table.cache[layTableId];
				for(var i=0, row; i < oldData.length; i++){
					row = oldData[i];
					if(!row || !row.tempId){
						oldData.splice(i, 1);    //删除一项
					}
					continue;
				}
				tableIns.reload({
					data : oldData
				});
			},
			save: function(){
                // 发送请求时添加 X-CSRFToken 头
				var oldData = table.cache[layTableId];
				var eid = JSON.parse(document.getElementById('eid').textContent);
				var tb = JSON.parse(document.getElementById('tb').textContent);
				console.log(oldData);
				for(var i=0, row; i < oldData.length; i++){
					row = oldData[i];
					if(!row.type){
						layer.msg("检查每一行，请选择分类！", { icon: 5 }); //提示
						return;
					}
				}
				$.ajax({
                      url:'./',
                      method:'post',
                      data:{eid:eid,tb:tb,data:JSON.stringify(table.cache[layTableId], null, 2)},
                      dataType:'JSON',
                      beforeSend: function(xhr) {
                        var csrfToken = document.querySelector('input[name=csrfmiddlewaretoken]').value;
                        xhr.setRequestHeader('X-CSRFToken', csrfToken);
                        },
                                            //请求的页面响应成功，则进行处理：
                      success:function(res){
                          if(res.code==0){
                              //弹出提示，并在1秒后进行跳转
                              layer.msg("提交成功",{icon: 1, time: 1000},function(){
                                  layer.msg(res.msg);
                                  location.href='../?eid='+eid +'&table='+tb;
                                  return false;
                              });
                          }else{
                              layer.msg("登录失败:" + res.msg);
                              return false;
                          }
                      },
                      //请求的页面响应失败，则进行处理：
                      error:function (data) {
                          layer.msg(JSON.stringify(data.field),function(){
                              location.reload();
                          });
                          return false;
                      }
                      //请求的页面响应成功，则进行处理：

                  });
				document.getElementById("jsonResult").innerHTML = JSON.stringify(table.cache[layTableId], null, 2);	//使用JSON.stringify() 格式化输出JSON字符串
			}
		}

		//激活事件
		var activeByType = function (type, arg) {
			if(arguments.length === 2){
				active[type] ? active[type].call(this, arg) : '';
			}else{
				active[type] ? active[type].call(this) : '';
			}
		}

		//注册按钮事件
		$('.layui-btn[data-type]').on('click', function () {
			var type = $(this).data('type');
			activeByType(type);
		});

		//监听select下拉选中事件
		form.on('select(type)', function(data){
			var elem = data.elem; //得到select原始DOM对象
			$(elem).prev("a[lay-event='type']").trigger("click");
		});

		 //监听工具条
		table.on('tool(dataTable)', function (obj) {
			var data = obj.data, event = obj.event, tr = obj.tr; //获得当前行 tr 的DOM对象;
			var tbData = viewObj.tbData;
			console.log(data);
			switch(event){
				case "type":
					//console.log(data);
					var select = tr.find("select[name='type']");
					if(select){
						var selectedVal = select.val();
						if(!selectedVal){
							layer.tips("请选择一个分类", select.next('.layui-form-select'), { tips: [3, '#FF5722'] }); //吸附提示
						}
						console.log(selectedVal);
						$.extend(obj.data, {'type': selectedVal});
						activeByType('updateRow', obj.data);	//更新行记录对象
					}
					break;
				case "state":
					var stateVal = tr.find("input[name='state']").prop('checked') ? 1 : 0;
//					$.extend(obj.data, {'state': stateVal})
//					activeByType('updateRow', obj.data);	//更新行记录对象
                    viewObj.tbData[tr.index()].state = stateVal;
					break;
				case "up":
                     if ($(tr).prev().html() == null) {
                            layer.msg("已经是最顶部了");
                            return;
                        }else{
                            var tem = tbData[tr.index()];
                            var tem2 = tbData[tr.prev().index()];
                            $(tr).insertBefore($(tr).prev());
                            viewObj.tbData[tr.index()] = tem;
                            viewObj.tbData[tr.next().index()] = tem2;
                            // 将本身插入到目标tr之前
                            //document.getElementById("jsonResult").innerHTML = JSON.stringify(viewObj.tbData, null, 2);
                            // 上移之后，数据交换
                            break;
                        };
                case "down":
                    if ($(tr).next().html() == null) {
                            layer.msg("已经是最底部了");
                            return;
                        } else{
                            var tem = tbData[tr.index()];
                            var tem2 = tbData[tr.next().index()];
                            $(tr).insertAfter($(tr).next());
                            viewObj.tbData[tr.index()] = tem;
                            viewObj.tbData[tr.prev().index()] = tem2;
                            // 将本身插入到目标tr之前
                            //document.getElementById("jsonResult").innerHTML = JSON.stringify(viewObj.tbData, null, 2);
                            // 上移之后，数据交换
                            break;
                        };
			}
		});
	});