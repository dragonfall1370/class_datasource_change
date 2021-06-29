# -*- coding: utf-8 -*-
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import NoAlertPresentException
import unittest, time, re
from selenium.webdriver.common.action_chains import ActionChains

class AppDynamicsJob(unittest.TestCase):
    def setUp(self):
        # AppDynamics will automatically override this web driver
        # as documented in https://docs.appdynamics.com/display/PRO44/Write+Your+First+Script
        self.driver = webdriver.Firefox()
        self.driver.implicitly_wait(30)
        self.base_url = "https://www.katalon.com/"
        self.verificationErrors = []
        self.accept_next_alert = True

    # def test_2(self):
    #     driver = webdriver.Firefox()
    #     driver.get("http://stackoverflow.com/questions/7794087/running-javascript-in-selenium-using-python")
    #     driver.execute_script("document.getElementsByClassName('comment-user')[0].click()")

    def test_app_dynamics_job(self):
        driver = self.driver
        #
        # open browser and login
        driver.get("https://tung.vincere.io/loginForm.do?redirectURI=%2F")
        driver.find_element_by_id("login-username").clear()
        driver.find_element_by_id("login-username").send_keys("tung.nguyen@vincere.io")
        driver.find_element_by_id("login-password").clear()
        driver.find_element_by_id("login-password").send_keys("conMiLo@#Google&21")
        driver.find_element_by_id("login-username").clear()
        driver.find_element_by_id("login-username").send_keys("sysadmin@vincere.io")
        driver.find_element_by_id("login-password").clear()
        driver.find_element_by_id("login-password").send_keys("emTAsuGheY!")
        driver.find_element_by_xpath("(.//*[normalize-space(text()) and normalize-space(.)='Forgot your password?'])[1]/preceding::button[1]").click()
        #
        # go to candidate dashboard
        driver.get("https://tung.vincere.io/candidateDashboard.do?tabId=1")
        #
        # go to a specified candidate
        driver.get("https://tung.vincere.io/candidateDashboard.do?tabId=1&id=579663")
        # time.sleep(10)
        # driver.execute_script("document.getElementsByClassName('left-inline-btn')[0].click()")
        driver.execute_script("openLocationDialog($element)")


        # driver.find_element_by_xpath("(.//*[normalize-space(text()) and normalize-space(.)='Current Address'])[1]/following::div[3]").click()

        # driver.find_element_by_id("address").click()
        # driver.find_element_by_id("address").clear()
        # driver.find_element_by_id("address").send_keys("Nonthaburi, 11000")
        # driver.find_element_by_xpath("(.//*[normalize-space(text()) and normalize-space(.)='Nonthaburi, 11000'])[2]/following::span[1]").click()
        # driver.find_element_by_xpath("(.//*[normalize-space(text()) and normalize-space(.)='Labels'])[1]/following::span[1]").click()
        # driver.find_element_by_xpath("(.//*[normalize-space(text()) and normalize-space(.)='Job actions'])[1]/following::span[2]").click()
        # driver.find_element_by_xpath("(.//*[normalize-space(text()) and normalize-space(.)='Jobs'])[1]/following::i[1]").click()
        # vcr_field_candidate_current_location > div:nth-child(2) > div:nth-child(1) > div:nth-child(1)

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
    unittest.main()
