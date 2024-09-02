import string

from django.shortcuts import render,redirect
import hashlib
import re
# Create your views here.
# from django.contrib.auth import authenticate,login
from django.http import HttpResponse
# from user.forms import RegForm  # 引入forms.py中定义的TitleSearch类
def loginview(request):
    return render(request, "user/wx-login.html")


def wechat_login(request):
    context = {
        'appid': 'wwae2c132f1c16f837',
        'redirect_uri': 'zxy-console.cn',  # 确保已经进行 URL 编码
        'state': '666'
    }
    return render(request, 'user/wx-login.html', context)

def user_login(request):
    print('<user_login>')
    if request.method == 'POST':
        token = request.POST.get('token', '')
        type = request.POST.get('type', 0)
    try:
        result = check_user_login(token, type)
    except Exception as e:
        return JsonResponse({'code': 1, 'message': str(e)})

    return JsonResponse({'code': 0, 'data': result})

def check_user_login(token, type):
    db26 = get_db('26_apollo')
    type = int(type)

    # 企业微信扫码模式
    if type == 0:
        # 先获得企业微信里的信息
        info = requests.get('https://auth.sh.nint.com/?caddy_wx_token=' + token)
        info = info.json()
        user_name = info['user_id']

    # 洗刷刷用户模式
    elif type == 1:
        # 先获得洗刷刷api里的信息
        info = requests.get('https://brush_test.ecdataway.com/out-link/check-token?token=' + token)
        info = info.json()
        code = info['code']
        if int(code) == 1:
            user_name = info['user_name']
        else:
            return '没有找到该用户'

    # 确保记录在用户表
    sql = "SELECT count(*) FROM graph.new_cleaner_user where user_name = '{user_name}' ".format(user_name=user_name)
    search = db26.query_all(sql)
    if search[0][0] == 0:
        sql = "INSERT INTO graph.new_cleaner_user (user_name,type) values('{user_name}','{type}') ".format(
            user_name=user_name, type=type)
        db26.execute(sql)
        # 并且如果是新加的用户，而且是 洗刷刷用户模式，则在 graph.new_cleaner_user_auth_awemeid_group 中 添加为 group_id = 1 的组员
        if type == 1:
            sql = "SELECT id,user_name FROM graph.new_cleaner_user where user_name = '{user_name}' ".format(
                user_name=user_name)
            user_result = db26.query_all(sql)
            uid = user_result[0][0]  # 先查出 uid
            sql = "INSERT INTO graph.new_cleaner_user_auth_awemeid_group (group_id,group_name,uid,del_flag) values(1,'测试1',{uid},0) ".format(
                uid=uid)
            db26.execute(sql)

    # 传给前端的 user_id 和 user_name
    sql = "SELECT id,user_name FROM graph.new_cleaner_user where user_name = '{user_name}' ".format(user_name=user_name)
    user_result = db26.query_all(sql)
    user_id = user_result[0][0]
    user_name = user_result[0][1]

    if_admin = function_if_admin(user_id)
    if if_admin == 0:
        user_role = 'user'
    elif if_admin == 1:
        user_role = 'admin'
    elif if_admin == 2:
        user_role = 'super'

    result = {'user_id': user_id, 'user_name': user_name, 'user_role': user_role}

    # token存入 session 中并且记录时间为7天
    r = get_cache()
    r.hmset(token, {'user_id': user_id, 'user_name': user_name, })
    r.expire(token, 86400 * 7)

    return result

def login(request,room_name=False):
    if request.method == "GET":
        nexturl = request.GET.get("next", "")
        print(request.GET.keys())
        print(request.user.is_authenticated)
        if request.user.is_authenticated:
            print(request.user.username)
            if nexturl:
                # login(request,request.user)
                return redirect(nexturl)
            else:
                nexturl = '/my-console/'
                return redirect(nexturl)
        else:  # 判断用户是否登录成功
            return render(request, 'my_console/page/login-1.html', {'nexturl': nexturl})

    if request.method == "POST":
        try:
            username = request.POST['username']
            password = request.POST['password']
            m=hashlib.md5()
            m.update(password.encode())
            password_m = m.hexdigest()
            user = UserInfo.objects.get(username=username, password=password_m)
            # print(username,password_m)
        except:
            msg = '请检查用户名或密码！'
            return render(request, "login-1.html", locals())

        # print(user)
        if user is not None:
            # msg = '登录成功'
            try:
                room_name = re.findall(r'[\w]{0,62}',request.POST["nexturl"])[3]
            except:
                room_name = username
            return render(request, 'chat/test.html', {
                "room_name": room_name,
                "user_name": username
            })
            # return render(request, "/chat/%s" % room_name,{"username":username})
            # return HttpResponse("登录成功")
    else:
        return render(request, "my_console/page/login-1.html", locals())

def register(request):
    # 用户注册逻辑代码
    if 'HTTP_X_FORWARDED_FOR' in request.META:
        host = request.META['HTTP_X_FORWARDED_FOR']
    else:
        host = request.META['REMOTE_ADDR']
    print(host)
    if request.method == 'GET':
        return render(request, 'register.html')
    elif request.method == 'POST':
        # 处理提交数据
        username = request.POST.get('username')
        email = request.POST.get('email')
        form = RegForm({'username': username})
        if not username:
            print(username,email)
            username = ''
            email = ''
            msg = '请输入正确的用户名'
            print(msg)
            return render(request, 'register.html', locals())
        if not form.is_valid():
            username = ''
            email = ''
            msg = '你注册的用户名字符太长了'
            return render(request, 'register.html', locals())
        for i in username:
            if i not in string.ascii_lowercase+string.ascii_uppercase:
                username = ''
                email = ''
                msg = '抱歉,用户名必须是英文字符串,不能含有中文、特殊字符等'
                return render(request, 'register.html', locals())
        password_1 = request.POST.get('password1')
        # 1 生成hash算法对象对密码进行加密
        m = hashlib.md5()
        # 2 对待加密明文使用update方法！要求输入明文为字节串
        m.update(password_1.encode())
        # 3 调用对象的 hexdigest[16进制],通常存16进制
        password_m1 = m.hexdigest()
        print(password_m1)  # 加密后的密文会显示在终端上
        password_2 = request.POST.get('password2')
        # 对password_2执行MD5加密处理
        m = hashlib.md5()
        m.update(password_2.encode())
        password_m2 = m.hexdigest()
        print(password_m2)
        # 可以设定密码格式，判断是都符合
        if not password_m1 or not password_m2:
            msg = '请输入正确的密码'
            return render(request, 'register.html', locals())
        # 判断两次密码输入是否一致
        if password_m1 != password_m2:
            msg = '两次密码不一致'
            return render(request, 'register.html', locals())
        # 查询用户名是否已注册过
        try:
            old_user = UserInfo.objects.get(username=username)
            # 当前用户名已被注册
            msg = '用户已经被注册 !'
            return render(request, 'register.html', locals())
        except Exception as e:
            # 若没查到的情况下进行报错，则证明当前用户名可用
            print('%s是可用用户名--%s' % (username, e))
            try:
                user = UserInfo.objects.create(username=username, password=password_m1, email=email,ip=host)
                # 注册成功后
                # html = '''
                # 注册成功 点击<a href='login/'>进入首页</a>
                # '''
                # 存session
                request.session['username'] = username
                msg = '%s已经注册成功，请登录！' % username
                return render(request, 'login.html', locals())
            # 若创建不成功会抛出异常
            except Exception as e:
                # 还可能存在用户名被重复使用的情况
                print(e)
                msg = '该用户名已经被占用 '
                return render(request, 'register.html', locals())

def get_auth_url():
    redirect_uri = '10.21.90.130:8667/my-console/'
    state = '1'
    corp_id = 'wwae2c132f1c16f837'
    auth_url = f"https://open.weixin.qq.com/connect/oauth2/authorize?appid={corp_id}&redirect_uri={redirect_uri}&response_type=code&scope=snsapi_base&state={state}#wechat_redirect"

    return auth_url


def get_user_info(code):
    agent_id = '1000002'
    secret = 'eZbsQYfQ4MjJXDl7MRYvxBFpbuLe8zzkGLPOz673Qjs'
    corp_id = 'wwae2c132f1c16f837'

    # 获取 access_token
    token_url = f"https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={corp_id}&corpsecret={secret}"
    token_response = requests.get(token_url)
    access_token = token_response.json().get('access_token')

    # 使用 code 获取 user_ticket
    auth_info_url = f"https://qyapi.weixin.qq.com/cgi-bin/user/getuserinfo?access_token={access_token}&code={code}"
    auth_info_response = requests.get(auth_info_url)
    user_ticket = auth_info_response.json().get('user_ticket')

    # 使用 user_ticket 获取用户详细信息
    user_info_url = f"https://qyapi.weixin.qq.com/cgi-bin/user/getuserdetail?access_token={access_token}"
    user_info_response = requests.post(user_info_entry, json={'user_ticket': user_ticket})
    user_info = user_info_response.json()

    return user_info