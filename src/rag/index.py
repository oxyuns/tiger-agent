import pandas as pd
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

def import_sources_to_mongodb():
    # MongoDB 연결
    client = MongoClient('mongodb://localhost:27017/')
    db = client['asianews']
    collection = db['source']
    
    try:
        # name 필드에 unique 인덱스 생성
        collection.create_index('name', unique=True)
        
        # CSV 파일 읽기
        df = pd.read_csv('source.csv')
        
        # 'lan' 컬럼을 'language'로 변경
        df = df.rename(columns={'lan': 'language'})
        
        # DataFrame을 dict 리스트로 변환
        sources = df.to_dict('records')
        
        # 각 소스를 MongoDB에 삽입
        for source in sources:
            try:
                # upsert 사용: 있으면 업데이트, 없으면 삽입
                collection.update_one(
                    {'name': source['name']},
                    {'$set': source},
                    upsert=True
                )
                print(f"Processed source: {source['name']}")
                
            except DuplicateKeyError:
                print(f"Duplicate source found for: {source['name']}")
            except Exception as e:
                print(f"Error processing source {source['name']}: {str(e)}")
        
        print("\nImport completed successfully!")
        
        # 결과 확인
        total_sources = collection.count_documents({})
        print(f"\nTotal sources in database: {total_sources}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    
    finally:
        # MongoDB 연결 종료
        client.close()
        print("\nMongoDB connection closed")

if __name__ == "__main__":
    import_sources_to_mongodb()