import os
import subprocess


def init_project_env(mode):
    """
    根据提供的模式（'dev'或'prod'）初始化项目环境。

    :param mode: 运行模式，'dev' 表示开发环境，'prod' 表示生产环境
    """
    if mode not in ['dev', 'prod']:
        print("错误：模式必须是 'dev' 或 'prod'")
        return

    if mode == 'dev':
        print("正在初始化开发环境...")
        setup_development_environment()
    elif mode == 'prod':
        print("正在初始化生产环境...")
        setup_production_environment()


def setup_development_environment():
    """
    设置开发环境特有的配置和依赖。
    默认你是windows嗷
    """
    print("配置开发环境变量(默认你是windows嗷)...")
    print("切换清华源...")
    subprocess.call(['pip', 'config', 'set', 'global.index-url', 'https://pypi.tuna.tsinghua.edu.cn/simple'])
    subprocess.call(['cmd', '/c', 'copy', f'{path}\\config\\DBConfigLocal.py', f'{path}\\config\\DBConfig.py'])
    subprocess.call(['pip', 'install', '-r', f'{path}\\requirements.txt'])


def setup_production_environment():
    """
    设置生产环境特有的配置和依赖。
    """
    print("配置生产环境变量(俺用的是python39)...")
    print("华为云默认源切换清华源...")
    subprocess.call(['pip3.9', 'config', 'set', 'global.index-url', 'https://pypi.tuna.tsinghua.edu.cn/simple'])
    subprocess.call(['cp', f'{path}/config/DBConfigProd.py', f'{path}/config/DBConfig.py'])
    subprocess.call(['python3.9', '-m', 'pip', 'install', '-r', 'requirements.txt'])


if __name__ == "__main__":
    path = os.path.abspath(os.path.dirname(__file__))
    # 用户输入选择环境模式
    mode = input("请输入运行模式（dev 或 prod）: ")
    init_project_env(mode)
