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
            return self.pattern.search(element_text)
        except StaleElementReferenceException:
            return False

class AppDynamicsJob():
    def setUp(self):
        # AppDynamics will automatically override this web driver
        # as documented in https://docs.appdynamics.com/display/PRO44/Write+Your+First+Script
        self.driver = webdriver.Firefox(executable_path=r'geckodriver.exe')
        self.driver.implicitly_wait(30)
        # self.base_url = "https://www.katalon.com/"
        self.verificationErrors = []
        self.accept_next_alert = True

    def get_latitude_longitude(self, addr_postcode):
        """
        get latitude longitude of address or poscode
        :param addr_postcode: 
        :return: (latitude, longitude) 
        """
        lat = r"/html/body/main/div[@class='row']/div/div[@class='row']/div/input[@id='lat']"
        lng = r"/html/body/main/div[@class='row']/div/div[@class='row']/div/input[@id='lng']"
        driver = self.driver
        driver.get("https://www.latlong.net/")
        try:
            driver.find_element_by_xpath(r"/html/body/main/div[@class='row']/div/form/input").send_keys(addr_postcode)
            driver.find_element_by_id("btnfind").click()
            # driver.implicitly_wait(10)
            # time.sleep(5)
            # element = WebDriverWait(driver, 5).until(
            #     wait_for_text_to_match((By.XPATH, lat), r'^(?!.*0\.000000).*$')  # wait until return val diff 0.000000
            # )

            t = driver.find_element_by_xpath(lat).get_attribute("value")
            pattern = re.compile(r'^(?!.*0\.000000).*$')
            while not pattern.search(t):
                time.sleep(5)

            return {'latitude': driver.find_element_by_xpath(lat).get_attribute("value"), 'longitude': driver.find_element_by_xpath(lng).get_attribute("value")}
        finally:
            pass
            # driver.quit()

    def convert_addr_to_latlong(self, addr_postcode):
        self.setUp()
        return self.get_latitude_longitude(addr_postcode)


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
