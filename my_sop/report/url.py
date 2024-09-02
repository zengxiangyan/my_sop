# -*- coding: utf-8 -*-
import pandas as pd
import tkinter as tk
from tkinter import filedialog
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import io
from PIL import Image
from io import BytesIO
from selenium import webdriver
from selenium.webdriver.common.by import By
# from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
# import os
# import subprocess
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def get_driver():

    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    driver = Chrome(service=Service(), options=chrome_options)

    # 注意：把chromedriver文件放到了当前文件夹里面，可以这样调用
    # driver = Chrome('./chromedriver.exe', options=chrome_options)
    return driver

def find_check_frame(driver):
    frame_name_list = ['bx-pu-qrcode-wrap']
    txt = ''
    for frame_name in frame_name_list:
        try:
            txt = driver.find_element(By.CLASS_NAME, num_class_name).text
            break
        except:
            time.sleep(1)
            continue
    return txt

def find_num_detail(driver):
    num_class_name_list = ['ItemHeader--salesDesc--srlk2Hv','ItemHeader--subTitle--17hJ88r','tb-sell-counter']
    num = '-'
    for num_class_name in num_class_name_list:
        try:
            num = driver.find_element(By.CLASS_NAME, num_class_name).text
            break
        except:
            time.sleep(1)
            continue
    return num

def find_price_detail(driver):
    price_class_name_list = ['Price--priceText--2nLbVda','Price--originPrice--1aJmU68','tb-property-cont']
    price = '-'
    for price_class_name in price_class_name_list:
        try:
            price = driver.find_element(By.CLASS_NAME, price_class_name).text
            break
        except:
            time.sleep(1)
            continue
    return price

def find_preferential_price(driver):
    preferential_price_class_name_list = ['Price--priceText--2nLbVda Price--extraPriceText--2dbLkGw','Price--extraPrice--2qsGsY3','tb-promo-price']
    preferential_price = '-'
    for preferential_price_class_name in preferential_price_class_name_list:
        try:
            preferential_price = driver.find_element(By.CLASS_NAME, preferential_price_class_name).text
            break
        except:
            time.sleep(1)
            continue
    return preferential_price

def get_ele_text(driver, xpath):
    try:
        ele_text = driver.find_element(By.XPATH, env_xpath(xpath)).text
        return ele_text
    except:
        try:
            ele_text = driver.find_element(By.XPATH, env_xpath("立即购买")).text
            return ele_text
        except:
            return ''


def get_product_info(driver):
    try:
        num = driver.find_element(By.XPATH, '/html/body/div[3]/div/div[2]/div[2]/div[1]/div/div[2]/div[1]/div/span').text
    except:
        num = ''
    try:
        org_price = driver.find_element(By.XPATH, '/html/body/div[3]/div/div[2]/div[2]/div[1]/div/div[2]/div[2]/div/div/div/div/div/span[3]').text
    except:
        org_price = ''
    try:
        now_price = driver.find_element(By.XPATH, '/html/body/div[3]/div/div[2]/div[2]/div[1]/div/div[2]/div[2]/div/div/div/div/div[2]/span[3]').text
    except:
        now_price = ''
    print([url,num,org_price,now_price])

def run_test(url):
    driver = get_driver()
    driver.get(url)
    num = find_num_detail(driver)
    price = find_price_detail(driver)
    preferential_price = find_preferential_price(driver)
    print(num,price,preferential_price,url)

def screenshot(driver):
    try:
        driver.find_element(By.CLASS_NAME, 'product-intro.clearfix').screenshot(
            file_path + '\\' + str(item_id[i]) + ".jpg")
    except:
        try:
            driver.find_element(By.CLASS_NAME, 'BasicContent--main--2mfq-dl').screenshot(
                file_path + '\\' + str(item_id[i]) + ".jpg")
        except:
            try:
                driver.find_element(By.CLASS_NAME, 'tm-clear').screenshot(file_path + '\\' + str(item_id[i]) + ".jpg")
            except:
                try:
                    driver.find_element(By.CLASS_NAME, 'itemInfo-wrap').screenshot(
                        file_path + '\\' + str(item_id[i]) + ".jpg")
                except:
                    driver.save_screenshot(file_path + '\\' + str(item_id[i]) + ".jpg")
                    print(str(item_id[i]) + "截的是全屏")
def run(retry_max=5):
    file = get_file()
    if '.csv' in file:
        df = pd.read_csv(file)
        output = file.replace('.csv','(已爬取).csv')
    elif '.xlsx' in file:
        df = pd.read_excel(file)
        output = file.replace('.xlsx', '(已爬取).csv')
    else:
        print("文件类型不支持，请重新选择csv/xlsx文件。")
        return
    if '页面销量' not in df.columns:
        df['页面销量'] = '-'
        df['页面划线价'] = '-'
        df['页面促销价'] = '-'
    urls = df.loc[df['页面销量']=='-']['hyperlink']
    driver = get_driver()

    for url in urls:
        try:
            driver.get(url)
            if find_check_frame(driver) != '':
                check = input('滑块是否已通过:\n1:通过\n0:未通过(会提前结束程序)')
                if check == '1':
                    continue
                else:
                    break
            num = find_num_detail(driver)
            price = find_price_detail(driver)
            preferential_price = find_preferential_price(driver)
            df.loc[df['hyperlink']==url,'页面销量'] = num
            df.loc[df['hyperlink'] == url, '页面划线价'] = price
            df.loc[df['hyperlink'] == url, '页面促销价'] = preferential_price
            # print(num,price,preferential_price,url)
            time.sleep(3)
        except:
            print(url,"解析错误")

    df.to_csv(output,index=False,encoding='utf-8-sig')
    return 1

def get_file():
    # 创建一个Tkinter根窗口并隐藏
    root = tk.Tk()
    root.withdraw()

    # 弹出文件选择对话框
    file_path = filedialog.askopenfilename()

    # 输出选定的文件路径
    print("Selected file:", file_path)
    return file_path


from PIL import Image


def ResizeImage(filein, fileout, scale=1):
    """
    改变图片大小
    :param filein: 输入图片
    :param fileout: 输出图片
    :param width: 输出图片宽度
    :param height: 输出图片宽度
    :param type: 输出图片类型（png, gif, jpeg...）
    :return:
    """
    img = Image.open(filein)
    width = int(img.size[0] * scale)
    height = int(img.size[1] * scale)
    type = img.format
    out = img.resize((width, height), Image.LANCZOS)
    # 第二个参数：
    # Image.NEAREST ：低质量
    # Image.BILINEAR：双线性
    # Image.BICUBIC ：三次样条插值
    # Image.ANTIALIAS：高质量
    out.save(fileout, type)


if __name__ == "__main__":
    # os.system("chrome_start.cmd")
    # chrome.exe --remote-debugging-port=9222 --user-data-dir="D:\selenium-chrome"

    # 设置命令行命令
    # cmd = ["start","","C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"] ##手动打开
    # # 使用 subprocess.Popen 执行命令
    # # 使用 subprocess.Popen 执行命令，不等待其完成
    # process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)
    # file_path = r'C:\Users\zeng.xiangyan\Downloads\\'
    # file_name = '天猫链接爬虫测试.xlsx'
    # sheet_name ='天猫链接爬虫测试'
    # run()
    # get_file()
    # run_test('https://detail.tmall.com/item.htm?id=610356950738')/
    driver = get_driver()
     # 打开网页
    driver.get('https://npcitem.jd.hk/27427527399.html')
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    # driver.refresh()
    # time.sleep(10)
    # WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    # # 发送F5键刷新页面
    # driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.F5)

    driver.execute_script("location.reload();")

    # 等待页面加载完成
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    # driver.back()
    #
    # # 等待页面加载完成
    # WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    # 设置想要截图的区域（左上角x坐标，左上角y坐标，右下角x坐标，右下角y坐标）
    left = (driver.find_element(By.CLASS_NAME, 'logo').location['x']-5.5)*1.25
    top = (driver.find_element(By.CLASS_NAME, 'logo').location['y']-5.5)*1.25
    right = (driver.execute_script("return document.documentElement.scrollWidth;")-left)*1.285
    bottom = (driver.find_element(By.CLASS_NAME, 'service-info').location['y'])*1.25+30
    print(left,top,right,bottom)
    # 根据指定区域裁剪图像
    # 定位页面的<body>标签
    body_element = driver.find_element(By.TAG_NAME, 'body')

    # 发送HOME键，滚动到页面顶部
    body_element.send_keys(Keys.HOME)

    # 等待页面滚动动画完成
    time.sleep(1)
    screenshot = driver.get_screenshot_as_png()
    # 将屏幕截图数据加载到一个Pillow图像对象
    screenshot = Image.open(BytesIO(screenshot))
    cropped_screenshot = screenshot.crop((left, top, right, bottom))

    # 保存裁剪后的图像
    cropped_screenshot.save('cropped_screenshot.png')
    # # 将图像转换为RGB模式  保存为高质量的JPEG
    # if cropped_screenshot.mode == 'RGBA':
    #     cropped_screenshot = cropped_screenshot.convert('RGB')
    # cropped_screenshot.save('cropped_screenshot.jpg', 'JPEG', quality=95)
    filein = r'cropped_screenshot.png'
    fileout = r'cropped_screenshot.png'
    ResizeImage(filein, fileout, scale=4)
    # 或者保存为PNG，PNG是无损的，通常质量更高

    # 关闭WebDriver
    driver.quit()
