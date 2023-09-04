import inspect
import os


def get_file_dir():
    filename = inspect.getframeinfo(inspect.currentframe()).filename
    path = os.path.dirname(os.path.abspath(filename))
    return path


def execute_captcha(page):
    try:
        page.locator("input[class=CheckboxCaptcha-Button]").click(timeout=2000)
        print("Enter the captcha text:")
        captcha_text = input()
        page.locator("input[class=Textinput-Control]").input_value(captcha_text)
        page.locator("button[class=CaptchaButton CaptchaButton_view_action]").click()
    except:
        None
