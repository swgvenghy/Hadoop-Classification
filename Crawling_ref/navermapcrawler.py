import time
import platform
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class NaverMapCrawler:
    def __init__(self):
        """크롤러 초기화"""
        self.arch = platform.machine()
        self.chrome_options = self._set_chrome_options()
        self.driver = self._initialize_driver(self.arch)
        self.wait = WebDriverWait(self.driver, 10)
        self.reviews_data = []  # 리뷰 데이터 저장용 리스트
        
    def _set_chrome_options(self):
        """크롬 브라우저 옵션 설정"""
        options = Options()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-extensions")
        return options
    
    def _initialize_driver(self, arch):
        """웹드라이버 초기화"""
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=self.chrome_options)
    
    def crawl_reviews(self, url):
        """리뷰 크롤링 실행"""
        try:
            self.driver.get(url)
            self._switch_to_iframe()
            self._click_review_tab()
            self._load_all_reviews()
            return self._extract_reviews()  # 리뷰 데이터 반환 추가
        except Exception as e:
            print(f"크롤링 중 오류 발생: {e}")
            return []  # 오류 발생시 빈 리스트 반환
        finally:
            self.driver.quit()
            
    def _switch_to_iframe(self):
        """iframe으로 전환"""
        iframe = self.wait.until(
            EC.presence_of_element_located((By.ID, "entryIframe"))
        )
        self.driver.switch_to.frame(iframe)
        
    def _click_review_tab(self):
        """리뷰 탭 클릭"""
        review_tab = self.wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="app-root"]/div/div/div/div[4]/div/div/div/div/a[2]')  # XPATH 개선
            )
        )
        review_tab.click()
        time.sleep(2)
        
    def _load_all_reviews(self):
        """모든 리뷰 로드"""
        while True:
            try:
                more_button = self.wait.until(
                    EC.presence_of_element_located(
                        (By.XPATH, '//*[@id="app-root"]/div/div/div/div[6]/div[3]/div[3]/div[2]/div/a')
                    )
                )
                self.driver.execute_script("arguments[0].scrollIntoView(true);", more_button)
                more_button.click()
                time.sleep(1)
            except:
                print("모든 리뷰를 불러왔습니다.")
                break

    def _extract_reviews(self):
        """모든 리뷰 데이터를 추출"""
        review_elements = self.driver.find_elements(By.CSS_SELECTOR, 'ul > li.pui__X35jYm')
        for review in review_elements:
            try:
                review_data = {
                    'date': review.find_element(By.CSS_SELECTOR, '.pui__QztK4Q .Vk05k span:nth-child(1) span:nth-child(3)').text,
                    'user_id': review.find_element(By.CSS_SELECTOR, '.pui__NMi-Dp').text,
                    'review_text': review.find_element(By.CSS_SELECTOR, '.pui__xtsQN-').text
                }
                self.reviews_data.append(review_data)
            except Exception as e:
                print(f"리뷰 추출 중 오류 발생: {e}")
                continue

        return self.reviews_data

if __name__ == "__main__":
    crawler = NaverMapCrawler()
    reviews = crawler.crawl_reviews('https://naver.me/IgDOya6r')
    # 결과를 DataFrame으로 변환하여 CSV로 저장
    if reviews:
        df = pd.DataFrame(reviews)
        df.to_csv('data/naver_reviews.csv', index=False, encoding='utf-8-sig')
