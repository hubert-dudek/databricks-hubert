# Databricks notebook source
# MAGIC %md
# MAGIC **Selenium chrome driver on databricks cluster**

# COMMAND ----------

# MAGIC %md
# MAGIC First we need to check latest version of chromedriver-binary on page https://pypi.org/project/chromedriver-binary/. In moment when I wam writing it is version 107.0. Than we need to go to our cluster in "Compute" and in "Libraries" select "Install new" and choose 

# COMMAND ----------

100.0.4896.20/chromedriver_linux64.zip

# COMMAND ----------

# MAGIC %md we need to install the same version od chromium-browser as chromedriver. First we check latest version on https://chromedriver.storage.googleapis.com/ and than look for corresponing ubuntu package using search

# COMMAND ----------

https://chromedriver.storage.googleapis.com/

# COMMAND ----------

# MAGIC %md we will use version X and will put it to bash script which will install it. Please run bellow cell to save it in DBFS.

# COMMAND ----------

https://pkgs.org/search/?q=chromium-browser



# COMMAND ----------

    dbutils.fs.mkdirs("dbfs:/databricks/scripts/")
    dbutils.fs.put("/databricks/scripts/selenium-install.sh","""
    #!/bin/bash
    apt-get update
    apt-get install chromium-browser=91.0.4472.101-0ubuntu0.18.04.1 --yes
    wget https://chromedriver.storage.googleapis.com/91.0.4472.101/chromedriver_linux64.zip -O /tmp/chromedriver.zip
    mkdir /tmp/chromedriver
    unzip /tmp/chromedriver.zip -d /tmp/chromedriver/
    """, True)
    display(dbutils.fs.ls("dbfs:/databricks/scripts/"))

# COMMAND ----------

# MAGIC %md
# MAGIC Please add "/databricks/scripts/selenium-install.sh" as starting script - init in cluster config.
# MAGIC Later in the notebook, you can use chrome, as in the below example.

# COMMAND ----------

    from selenium import webdriver
    chrome_driver = '/tmp/chromedriver/chromedriver'
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--headless')
    # chrome_options.add_argument('--disable-dev-shm-usage') 
    chrome_options.add_argument('--homedir=/dbfs/tmp')
    chrome_options.add_argument('--user-data-dir=/dbfs/selenium')
    # prefs = {"download.default_directory":"/dbfs/tmp",
    #          "download.prompt_for_download":False
    # }
    # chrome_options.add_experimental_option("prefs",prefs)
    driver = webdriver.Chrome(executable_path=chrome_driver, options=chrome_options)

# COMMAND ----------

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

driver = webdriver.Firefox()
driver.get("http://www.python.org")
assert "Python" in driver.title
elem = driver.find_element(By.NAME, "q")
elem.clear()
elem.send_keys("pycon")
elem.send_keys(Keys.RETURN)
assert "No results found." not in driver.page_source
driver.close()

# COMMAND ----------



# COMMAND ----------

driver.execute("get", {'url': 'https://www.booking.com/searchresults.en-gb.html?ss=Prague%2C+Czech+Republic&efdco=1&label=gen173nr-1BCAEoggI46AdIM1gEaDqIAQGYAQm4ARfIAQzYAQHoAQGIAgGoAgO4ArGQiZsGwAIB0gIkMDhlNzY3NTctMDFlNi00Mzg2LTljYWItMDRkNzVmZjk1NzAx2AIF4AIB&sid=118868bec0fcfe0afc53985fa9ba52e3&aid=304142&lang=en-gb&sb=1&src_elem=sb&src=searchresults&checkin=2022-12-01&checkout=2022-12-01&group_adults=2&no_rooms=1&group_children=0&sb_travel_purpose=leisure'})
hotels = [elem.text for elem in WebDriverWait(driver, 20).until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, "div[data-testid='title']")))]
prices = [elem.text for elem in WebDriverWait(driver, 20).until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, "div[data-testid='price-and-discounted-price'] > span")))]
schema = StructType(
    [StructField("hotels", StringType()),
     StructField("prices", StringType())])
df = spark.createDataFrame([hotels, prices],schema=Schema)
display(df)

# COMMAND ----------

Alternative solution: if we just need to download html file we can use SparkContext.addFile() or just use requests
iF WE need just to parse html without simulating user actions (like clicks, mouse moves) it is ismple to use BeautifulSoap
