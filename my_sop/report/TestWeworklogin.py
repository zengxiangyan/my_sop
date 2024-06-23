# -*- coding: utf-8 -*-
import time

import yaml
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


class TestWeworkLogin:

    def setup_class(self):
        # 设置 Chrome 选项
        chrome_options = Options()
        # chrome_options.add_argument("--headless")  # 无头模式，可选
        # chrome_options.add_argument("--disable-dev-shm-usage")
        # chrome_options.add_argument("--no-sandbox")
        # 创建 Chrome Service
        service = Service(ChromeDriverManager().install())

        # 创建 Chrome WebDriver，并传递 Chrome 选项和 Service 对象
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.driver.implicitly_wait(10)
        self.driver.maximize_window()

    def teardown_class(self):
        self.driver.quit()


    def test_save_cookies(self):
        """
        保存cookies
        :return:
        """
        # 1.访问SOP的登录页面
        self.driver.get("https://sop.ecdataway.com/site/login")
        # 2.输入用户名及密码
        self.driver.find_element(By.XPATH, "/html/body/div/div[2]/div/form/div[1]/table/tbody/tr[2]/td/div/div/input[1]").send_keys('zeng.xiangyang')
        self.driver.find_element(By.XPATH, "/html/body/div/div[2]/div/form/div[1]/table/tbody/tr[4]/td/div/div/input").send_keys('13639054279zXY')
        # 3.登录并获取浏览器的cookies
        self.driver.find_element(By.XPATH, "/html/body/div/div[2]/div/form/div[1]/table/tbody/tr[5]/td/div[2]/span[1]").click()
        self.driver.find_element(By.XPATH, "/html/body/div/div[2]/div/form/div[2]/span/a").click()
        time.sleep(3.5)
        self.driver.get("https://sop.ecdataway.com/bi/reportform/getreportnew2?&eid=92080&batchId=264")
        wait = WebDriverWait(self.driver, 10)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))

        # 查找包含 csrf-token 的 meta 标签
        csrf_meta = self.driver.find_element(By.CSS_SELECTOR, 'meta[name="csrf-token"]')

        # 获取 csrf-token 的值
        csrf_token = csrf_meta.get_attribute('content')
        csrf_token = {"csrf_token":csrf_token}
        # 保存 csrf-token 的值
        if csrf_token:
            print('csrf-token:', csrf_token)
            with open("../report/csrf_token.yaml", "w") as f:
                yaml.safe_dump(data=csrf_token, stream=f)
        else:
            print('csrf-token not found.')
        cookies = self.driver.get_cookies()
        # 4.保存cookies
        with open("../report/cookies.yaml", "w") as f:
            yaml.safe_dump(data=cookies, stream=f)


    def test_get_cookie(self):
        """
        植入cookie跳过登录
        :return:
        """
        # 1.访问sop登录页面
        self.driver.get("https://sop.ecdataway.com/site/login")
        # 2.获取本地的cookie
        with open("./report/cookies.yaml", "r") as f:
            cookies = yaml.safe_load(f)
        print(cookies)
        # 3.植入cookie
        for c in cookies:
            self.driver.add_cookie(c)
        # 4.访问sop查询接口
        self.driver.get("https://sop.ecdataway.com/bi/reportform/getreportnew2?&eid=92080&batchId=264")
        time.sleep(5)
if __name__ == '__main__':
    a = TestWeworkLogin()
    a.setup_class()
    a.test_save_cookies()