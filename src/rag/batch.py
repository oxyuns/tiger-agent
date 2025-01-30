import feedparser
from pymongo import MongoClient
from datetime import datetime
import time
import schedule
import logging
from typing import Optional, Dict, Any, Tuple
import pytz
from ollama import Client

# 로깅 설정
logging.basicConfig(
   level=logging.INFO,
   format='%(asctime)s - %(levelname)s - %(message)s'
)

class RSSCollector:
   def __init__(self):
       # MongoDB 연결
       self.client = MongoClient('mongodb://localhost:27017/')
       self.db = self.client['asianews']
       
       # Ollama 클라이언트 초기화
       self.ollama_client = Client(host='http://localhost:11434')
       
       # 컬렉션 설정 및 인덱스 생성
       self.source_collection = self.db['source']
       self.news_collection = self.db['news']
       self.news_collection.create_index('link', unique=True)
       self.news_collection.create_index('publishedDate')
       self.news_collection.create_index('source')

   def clean_text(self, text: str) -> str:
       """HTML 태그 제거 및 텍스트 정리"""
       return ' '.join(text.split()).strip()

   def is_crypto_related(self, title: str, description: str) -> bool:
       """제목과 내용이 암호화폐/Web3 관련인지 분석"""
       try:
           keywords = [
               # 일반 용어
               'crypto', 'cryptocurrency', 'cryptocurrencies',
               'virtual currency', 'virtual currencies',
               'digital currency', 'digital currencies',
               'digital asset', 'digital assets',
               'virtual asset', 'virtual assets',
               'digital token', 'digital tokens',
               'virtual token', 'virtual tokens',
               'tokenization', 'tokenisation',
               'custody', 'custodian', 'custodians',
               
               # 주요 암호화폐
               'bitcoin', 'btc', 'ethereum', 'eth', 'ripple', 'xrp', 'usdt', 'usdc',
               'stablecoin', 'altcoin',
               
               # 기술 용어
               'blockchain', 'web3', 'distributed ledger', 'smart contract',
               'dao', 'defi', 'gamefi', 'metaverse', 'mining', 'miner', 'staking',
               'nft', 'ico', 'ido', 'ieo', 'sto',
               
               # 거래소 관련
               'dex', 'cex', 'exchange', 'wallet', 'cold storage', 'hot wallet',
               
               # 주요 거래소/프로젝트
               'binance', 'coinbase', 'kraken', 'okx', 'kucoin', 'bitget',
               'metamask', 'opensea', 'uniswap', 'aave', 'compound',
               
               # 규제/제도 관련
               'cbdc', 'regulatory', 'sec cryptocurrency', 'crypto regulation',
               'virtual asset service provider', 'vasp'
           ]
           
           quick_check = any(keyword in title.lower() or keyword in description.lower() 
                           for keyword in keywords)
           
           if quick_check:
               keywords_found = [keyword for keyword in keywords 
                              if keyword in title.lower() or keyword in description.lower()]
               print(f"발견된 키워드: {keywords_found}")
               
               response = self.ollama_client.chat(
               model='deepseek-r1:7b',
               messages=[
                   {'role': 'system', 'content': '''You are a content classifier for crypto/blockchain news. Analyze if the content is:
                    1. Actually a news article (not an advertisement or promotional content)
                    2. Related to crypto/blockchain topics

                    IMPORTANT: Your response must follow this exact format:
                    <think>your analysis</think>
                    YES or NO

                    The answer must be a single word YES or NO on a new line after the think tag.'''},
                   {'role': 'user', 'content': f"Title: {title}\nDescription: {description}"}
               ]
               )

                # think 태그 이후의 마지막 줄에서 YES/NO 추출
               result = response.message.content.split('</think>')[-1].strip()
               return result.upper() == "YES"
           
           return False
               
       except Exception as e:
           logging.error(f"Crypto relevance analysis error: {str(e)}")
           return False

   def translate_text(self, text: str) -> str:
       """Deepseek를 사용하여 텍스트를 영어로 번역"""
       try:
           response = self.ollama_client.chat(
               model='deepseek-r1:7b',
               messages=[
                   {'role': 'system', 'content': 'You are a translator. Translate the following text to English, maintaining the original meaning and technical terms.'},
                   {'role': 'user', 'content': text}
               ]
           )
           return response.message.content.strip()
       except Exception as e:
           logging.error(f"Translation error: {str(e)}")
           return text

   def process_feed_entry(self, entry: Dict[str, Any], source: Dict[str, Any]) -> Optional[Dict[str, Any]]:
       """RSS 피드 항목 처리"""
       try:
           if 'link' not in entry:
               logging.warning("Skipping entry without link")
               return None

           existing_article = self.news_collection.find_one({'link': entry.get('link')})
           if existing_article:
               logging.info(f"Skipping duplicate article: {entry.get('link')}")
               return None

           print("\n=== 뉴스 처리 시작 ===")
           print(f"URL: {entry.get('link')}")
           
           title = entry.get('title', '')
           description = entry.get('description', '') or entry.get('summary', '')

           if not title or not description:
               logging.warning("Skipping entry without title or description")
               return None

           print(f"제목: {title}")
           print(f"Description: {description}")

           if source['language'] != 'en':
               logging.info(f"Translating from {source['language']}")
               title = self.translate_text(title)
               description = self.translate_text(description[:2000])
               print(f"번역된 제목: {title}")
               print(f"번역된 Description: {description}")

           print("\n크립토 관련성 체크 중...")
           is_related = self.is_crypto_related(title, description)
           if not is_related:
               print("결과: 크립토 무관 컨텐츠 ✗")
               print("===================\n")
               return None
           print("결과: 크립토 관련 뉴스 ✓")

           published_date = entry.get('published_parsed') or entry.get('updated_parsed')
           if published_date:
               published_date = datetime.fromtimestamp(time.mktime(published_date))
               published_date = pytz.UTC.localize(published_date)
           else:
               published_date = datetime.now(pytz.UTC)
           
           article = {
               'title': title,
               'description': description,
               'link': entry.get('link', ''),
               'source': source['name'],
               'sourceLanguage': source['language'],
               'country': source['country'],
               'category': source['category'],
               'publishedDate': published_date,
               'createdAt': datetime.now(pytz.UTC)
           }

           print("MongoDB에 저장 완료")
           print("===================\n")
           return article

       except Exception as e:
           logging.error(f"Error processing entry: {str(e)}")
           return None

   def collect_feeds(self):
       """모든 소스에서 피드 수집"""
       try:
           sources = self.source_collection.find({})
           
           for source in sources:
               logging.info(f"Collecting feed from: {source['name']}")
               
               try:
                   feed = feedparser.parse(source['url'])
                   
                   if feed.get('status') != 200:
                       logging.error(f"Failed to fetch feed from {source['name']}: Status {feed.get('status')}")
                       continue

                   for entry in feed.entries:
                       article = self.process_feed_entry(entry, source)
                       
                       if article:
                           try:
                               self.news_collection.insert_one(article)
                               logging.info(f"Inserted new crypto article: {article['title'][:50]}...")
                           except Exception as e:
                               logging.error(f"Error inserting article: {str(e)}")
                               continue
                   
                   logging.info(f"Completed collecting from {source['name']}")
                   
               except Exception as e:
                   logging.error(f"Error processing feed {source['name']}: {str(e)}")
                   continue

       except Exception as e:
           logging.error(f"Error in collect_feeds: {str(e)}")

   def run_scheduler(self):
       """60분마다 실행되는 스케줄러"""
       logging.info("Starting RSS collector scheduler")
       
       # 즉시 첫 실행
       self.collect_feeds()
       
       # 60분마다 실행되도록 스케줄 설정
       schedule.every(60).minutes.do(self.collect_feeds)
       
       while True:
           schedule.run_pending()
           time.sleep(1)

if __name__ == "__main__":
   collector = RSSCollector()
   collector.run_scheduler()