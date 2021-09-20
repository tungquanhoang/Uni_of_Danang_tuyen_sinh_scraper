from bs4 import BeautifulSoup
from selenium import webdriver
from multiprocessing import Process, Queue
from time import sleep
from datetime import datetime
import requests
import re
import pandas as pd
import openpyxl


op = webdriver.ChromeOptions()
op.add_argument('headless')

numbers = [x for x in range(2, 102)]
numbers = [str(item).zfill(2) for item in numbers]


def scraper(start_page, end_page, queue):
    driver = webdriver.Chrome("D:\Merlin\chromedriver.exe", options=op)
    danh_sach_sv = []
    for page in range(start_page, end_page + 1):
        print(page) # to keep track
        if page != 109:
            so_sv = len(numbers)
        else:
            so_sv = len(numbers) - 62 # last page (109) has 38 records

        for i in range(so_sv):

            driver.get("http://ketqua.udn.vn/Danhsach.aspx?madot=qgia1-2021&xem=0&truong=&fff=&iddot=40&Keyword=g7vn&Page=" + str(page))

            in_giay_bao = driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_Grid1_ctl' + numbers[i] + '_cmmDangky"]')
            driver.execute_script("arguments[0].click();", in_giay_bao)

            giay_bao = requests.get(driver.current_url)
            soup = BeautifulSoup(giay_bao.content, "html.parser")

            thong_tin_sv = {}
            thong_tin_sv['ho_ten'] = soup.find_all('b')[16].get_text()
            thong_tin_sv['ngay_sinh'] = soup.find_all('b')[17].get_text()
            thong_tin_sv['khu_vuc'] = soup.find_all('b')[19].get_text()
            thong_tin_sv['doi_tuong'] = soup.find_all('b')[20].get_text()
            thong_tin_sv['d1'] = soup.find_all('b')[21].get_text()
            thong_tin_sv['d2'] = soup.find_all('b')[22].get_text()
            thong_tin_sv['d3'] = soup.find_all('b')[23].get_text()
            thong_tin_sv['dxt'] = float(soup.find_all('b')[24].get_text())
            thong_tin_sv['nganh'] = soup.find_all('b')[25].get_text()
            thong_tin_sv['truong'] = soup.find_all('b')[26].get_text()

            # print(thong_tin_sv)

            danh_sach_sv.append(thong_tin_sv)

            driver.back()

    driver.close()

    df = pd.DataFrame(danh_sach_sv)

    queue.put(df)


def join_df(frames):
    frames = frames
    return pd.concat(frames)


if __name__ == "__main__":
    queue_dict = {}
    for x in range(1, 5):
        queue_dict['Q{0}'.format(x)] = Queue()

    targeted_pages = [1, 28, 55, 82, 109] # total 109 pages
    process_dict = {}
    for i in range(1, 5):
        process_dict['p{0}'.format(i)] = Process(target=scraper,
            args=(targeted_pages[i-1], targeted_pages[i], queue_dict['Q{0}'.format(i)]))

        sleep(10)

    print(datetime.now())

    for i in range(1, 5):
        process_dict['p{0}'.format(i)].start()
        sleep(10)

    frames = []
    for i in range(1, 5):
        df = queue_dict['Q{0}'.format(i)].get()
        frames.append(df)

    for i in range(1, 5):
        process_dict['p{0}'.format(i)].join()

    print(datetime.now())
    final_df = join_df(frames).reset_index()
    final_df.to_excel('DUE_trung_tuyen.xlsx')
    print("Done!")







