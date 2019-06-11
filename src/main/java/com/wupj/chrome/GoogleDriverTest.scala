package com.wupj.chrome

import java.io.File
import java.util.concurrent.TimeUnit

import org.openqa.selenium.chrome.{ChromeDriver, ChromeDriverService}

object GoogleDriverTest {
  def main(args: Array[String]): Unit = {

  }

  def yyeDemo(): Unit = {

    val chrome = getChromeDriver();
    // (访问目标页面，但是由于没有登录所以会进行登录)跳转到登录页面
    chrome.get("http://yytdemo.uf-tobacco.com/yyt-platform-manage/ui/index2018/index.html?funCode=CPHL0808&url=http%253A%252F%252Fyytdemo.uf-tobacco.com%252Fyyt-basedoc%252FCPHL%252FCPHL08%252FCPHL0808%252Findex.htm%253FfunCode%253DCPHL0808%2526billType%253DCPHL0808%2526funCode%253DCPHL0808&navName=%25E4%25BA%2591%25E5%25B9%25B3%25E5%258F%25B0")
    chrome.findElementById("username").sendKeys("wuzq")
    chrome.findElementById("password").sendKeys("1")
    chrome.findElementByName("submit").click()
    // 五秒
    chrome.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS)
    // 切换到目标frame，否则无法
    chrome.switchTo().frame("myiframe")
    chrome.findElementById("ext-gen14").click()
    // 执行脚本文件，给表单
    chrome.executeScript("$('#user_code').val('reboot_0001');$('#user_name').val('机器人001');$('#user_note').val('机器人用户一号');$('#user_password').val('123456789');$('#user_password_i').val('123456789');")
    // 点击保存用户操作
    //   chrome.findElementById("ext-gen18").click()
    Thread.sleep(10000)
    chrome.quit()
  }

  def song(): Unit = {
    val chrome = getChromeDriver()
    chrome.get("https://music.163.com/#/user/songs/rank?id=266725027")
    chrome.switchTo().frame("g_iframe")
  }

  def getChromeDriver(): ChromeDriver = {
    val service = new ChromeDriverService.Builder().usingDriverExecutable(new File("D:\\data\\Chrome\\chromedriver_win32\\chromedriver.exe"))
      .usingAnyFreePort().build()
    val chrome = new ChromeDriver(service)
    chrome
  }
}
