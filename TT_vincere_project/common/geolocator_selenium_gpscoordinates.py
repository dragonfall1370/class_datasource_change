# -*- coding: utf-8 -*-
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import NoAlertPresentException
import unittest, time, re
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import StaleElementReferenceException

import os
# os.environ['MOZ_HEADLESS'] = '1'  # set headless for firefox

import re
from selenium.webdriver.support import expected_conditions as EC

class wait_for_text_to_match(object):
    def __init__(self, locator, pattern):
        self.locator = locator
        self.pattern = re.compile(pattern)

    def __call__(self, driver):
        try:
            element_text = EC._find_element(driver, self.locator).get_attribute("value")
            # print(element_text)
            return self.pattern.search(element_text)
        except StaleElementReferenceException:
            return False

class AppDynamicsJob():
    def setUp(self):
        # # AppDynamics will automatically override this web driver
        # # as documented in https://docs.appdynamics.com/display/PRO44/Write+Your+First+Script
        # chrome_options = Options()
        # # chrome_options.headless = True
        # # chrome_options.add_argument("--headless")  # Runs Chrome in headless mode.
        # chrome_options.add_argument('--no-sandbox')  # Bypass OS security model
        # chrome_options.add_argument('--disable-gpu')  # applicable to windows os only
        # chrome_options.add_argument('start-maximized')  #
        # chrome_options.add_argument('disable-infobars')
        # chrome_options.add_argument("--disable-extensions")
        # self.driver = webdriver.Chrome(executable_path=r'chromedriver.exe', chrome_options=chrome_options)

        self.driver = webdriver.Firefox(executable_path=r'geckodriver.exe')
        self.driver.implicitly_wait(30)
        # self.base_url = "https://www.katalon.com/"
        self.verificationErrors = []
        self.accept_next_alert = True

    def get_latitude_longitude(self, addr_postcost):
        #url = 'https://www.gps-coordinates.net/'
        url = 'https://gps-coordinates.org/coordinate-converter.php'
        input = r"/html/body/div/div/div/div/form/div/div/input[@id='address']"
        submit_button = r"/html/body/div/div/div/div/form/div[@class='form-group'][2]/div/button"
        lat = r"/html/body/div/div/div/div/form/div/div//input[@id='latitude']"
        lng = r"/html/body/div/div/div/div/form/div/div//input[@id='longitude']"
        geocodedaddress = r"/html/body/div/div/div/div/div/div/div/div/div/div/div/div/div/div/div/div[@id='info_window']/span[@id='geocodedAddress']"
        driver = self.driver
        driver.get(url)
        # driver.find_element_by_id("ha3308").clear()
        # driver.find_element_by_id("ha3308").send_keys("EH54")
        # driver.find_element_by_id("ha3308").click()
        # driver.find_element_by_id("ha3308").clear()

        driver.find_element_by_xpath(lat).clear()
        driver.find_element_by_xpath(lng).clear()
        driver.find_element_by_xpath(input).clear()

        driver.find_element_by_xpath(lat).send_keys('0.000000')
        driver.find_element_by_xpath(lng).send_keys('0.000000')

        try:
            driver.find_element_by_xpath(input).send_keys(addr_postcost)
            driver.find_element_by_xpath(submit_button).click()
            # driver.implicitly_wait(10)
            # time.sleep(5)

            element = WebDriverWait(driver, 60).until(
                wait_for_text_to_match((By.XPATH, lat), r'^(?!.*0\.000000).*$')  # wait until return val diff 0.000000
            )
            element = WebDriverWait(driver, 60).until(
                EC.presence_of_all_elements_located((By.XPATH, geocodedaddress))
            )

            print(driver.find_element_by_xpath(lat).get_attribute("value"))
            print(driver.find_element_by_xpath(lng).get_attribute("value"))
            print(driver.find_element_by_xpath(geocodedaddress).get_attribute("value"))
        finally:
            print('')
            # driver.quit()

    def is_element_present(self, how, what):
        try:
            self.driver.find_element(by=how, value=what)
        except NoSuchElementException as e:
            return False
        return True

    def is_alert_present(self):
        try:
            self.driver.switch_to_alert()
        except NoAlertPresentException as e:
            return False
        return True

    def close_alert_and_get_its_text(self):
        try:
            alert = self.driver.switch_to_alert()
            alert_text = alert.text
            if self.accept_next_alert:
                alert.accept()
            else:
                alert.dismiss()
            return alert_text
        finally:
            self.accept_next_alert = True

    def tearDown(self):
        # To know more about the difference between verify and assert,
        # visit https://www.seleniumhq.org/docs/06_test_design_considerations.jsp#validating-results
        self.assertEqual([], self.verificationErrors)


if __name__ == "__main__":
    loc = AppDynamicsJob()
    loc.setUp()
    loc.get_latitude_longitude("46 Syme Place,Rosyth,Fife")
