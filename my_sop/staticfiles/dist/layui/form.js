<<<<<<< HEAD
// 定义变量来记录表单状态
var isFormHidden = false;

// 获取按钮元素
//var toggleFormBtn = document.getElementById("toggleFormBtn");
var togglesqlBtn = document.getElementById("togglesqlBtn");
var container = document.querySelector('.container');
var dividerIcon = document.querySelector('.divider-icon');
var sqlreview = document.getElementById("sqlreview");
var sql_card = document.getElementById("editorContainer");
// 注册点击事件监听器
//toggleFormBtn.addEventListener("click", function(event) {
//  event.preventDefault();
//  var form = document.getElementById("myForm");
//  if (isFormHidden) {
//    form.style.display = "block";
//    toggleFormBtn.innerHTML = '<i class="layui-icon layui-icon-up">隐藏筛选</i>';
//    isFormHidden = false;
//  } else {
//    form.style.display = "none";
//    toggleFormBtn.innerHTML = '<i class="layui-icon layui-icon-down">展开筛选</i>';
//    container.classList.toggle('columns-1-10');
//    isFormHidden = true;
//  }
//});
togglesqlBtn.addEventListener('click', function() {
  var form = document.getElementById("rule");
  // 切换表单状态
  if (isFormHidden) {
    setTimeout(function() {
      // 显示表单
      form.style.display = "block";
      // 更新按钮文本
      toggleFormBtn.innerHTML = '<i class="layui-icon layui-icon-up">隐藏筛选</i>';
      container.classList.toggle('columns-1-2');
      dividerIcon.classList.toggle('rotated'); // 切换旋转样式
      sqlreview.classList.toggle('maxwidth');
      sql_card.classList.toggle('maxwidth');
    }, 30); // 这里的300是过渡效果的时间，需要与CSS中的过渡时间保持一致
    // 更新表单状态
    isFormHidden = false;
  } else {
    setTimeout(function() {
      // 隐藏表单
      form.style.display = "none";
      // 更新按钮文本
      toggleFormBtn.innerHTML = '<i class="layui-icon layui-icon-down">展开筛选</i>';
      container.classList.toggle('columns-1-2');
      dividerIcon.classList.toggle('rotated'); // 切换旋转样式
      sqlreview.classList.toggle('maxwidth');
      sql_card.classList.toggle('maxwidth');
//      editor.resize();
    }, 30); // 这里的300是过渡效果的时间，需要与CSS中的过渡时间保持一致
    // 更新表单状态
    isFormHidden = true;
  }
});

var addButton = document.getElementById('add_sp');
// 监听按钮的点击事件
addButton.addEventListener('click', function(event) {
  event.preventDefault(); // 阻止按钮默认的提交行为
  // 发送AJAX请求
  var xhr = new XMLHttpRequest();
  var eid = JSON.parse(document.getElementById('eid').textContent);
  var tb = document.getElementById('table').value;
  window.location.href = window.location.protocol + '//' + window.location.host + '/sop_e/set_view_sp/?eid=' + eid + '&tb=' + tb;
//  xhr.send('action=set_view_sp'); // 替换成正确的请求参数
});

=======
// 定义变量来记录表单状态
var isFormHidden = false;

// 获取按钮元素
//var toggleFormBtn = document.getElementById("toggleFormBtn");
var togglesqlBtn = document.getElementById("togglesqlBtn");
var container = document.querySelector('.container');
var dividerIcon = document.querySelector('.divider-icon');
var sqlreview = document.getElementById("sqlreview");
var sql_card = document.getElementById("editorContainer");
// 注册点击事件监听器
//toggleFormBtn.addEventListener("click", function(event) {
//  event.preventDefault();
//  var form = document.getElementById("myForm");
//  if (isFormHidden) {
//    form.style.display = "block";
//    toggleFormBtn.innerHTML = '<i class="layui-icon layui-icon-up">隐藏筛选</i>';
//    isFormHidden = false;
//  } else {
//    form.style.display = "none";
//    toggleFormBtn.innerHTML = '<i class="layui-icon layui-icon-down">展开筛选</i>';
//    container.classList.toggle('columns-1-10');
//    isFormHidden = true;
//  }
//});
togglesqlBtn.addEventListener('click', function() {
  var form = document.getElementById("rule");
  // 切换表单状态
  if (isFormHidden) {
    setTimeout(function() {
      // 显示表单
      form.style.display = "block";
      // 更新按钮文本
      toggleFormBtn.innerHTML = '<i class="layui-icon layui-icon-up">隐藏筛选</i>';
      container.classList.toggle('columns-1-2');
      dividerIcon.classList.toggle('rotated'); // 切换旋转样式
      sqlreview.classList.toggle('maxwidth');
      sql_card.classList.toggle('maxwidth');
    }, 30); // 这里的300是过渡效果的时间，需要与CSS中的过渡时间保持一致
    // 更新表单状态
    isFormHidden = false;
  } else {
    setTimeout(function() {
      // 隐藏表单
      form.style.display = "none";
      // 更新按钮文本
      toggleFormBtn.innerHTML = '<i class="layui-icon layui-icon-down">展开筛选</i>';
      container.classList.toggle('columns-1-2');
      dividerIcon.classList.toggle('rotated'); // 切换旋转样式
      sqlreview.classList.toggle('maxwidth');
      sql_card.classList.toggle('maxwidth');
//      editor.resize();
    }, 30); // 这里的300是过渡效果的时间，需要与CSS中的过渡时间保持一致
    // 更新表单状态
    isFormHidden = true;
  }
});

var addButton = document.getElementById('add_sp');
// 监听按钮的点击事件
addButton.addEventListener('click', function(event) {
  event.preventDefault(); // 阻止按钮默认的提交行为
  // 发送AJAX请求
  var xhr = new XMLHttpRequest();
  var eid = JSON.parse(document.getElementById('eid').textContent);
  var tb = document.getElementById('table').value;
  window.location.href = window.location.protocol + '//' + window.location.host + '/sop_e/set_view_sp/?eid=' + eid + '&tb=' + tb;
//  xhr.send('action=set_view_sp'); // 替换成正确的请求参数
});

>>>>>>> c7ad5a18749c6af59354deb3728da04e8b425ab0
