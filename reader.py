import os
import json
from google.cloud import bigquery
import trafilatura
from google.oauth2 import service_account

# 1. 인증 설정 (GitHub Secrets 활용)
sa_info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
creds = service_account.Credentials.from_service_account_info(sa_info)
client = bigquery.Client(credentials=creds, project=sa_info['project_id'])

PROJECT_ID = sa_info['']
DATASET = "kinetic_field"

def run_pipeline():
    # Step A: 외부 테이블(시트) -> 네이티브 테이블 증분 동기화
    sync_sql = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET}.raw_stream_native` 
    (published_at, source, title, url, url_canonical, tags, title_hash, simhash, duplicate_of)
    SELECT published_at, source, title, url, url_canonical, tags, title_hash, simhash, duplicate_of
    FROM `{PROJECT_ID}.{DATASET}.raw_stream_entry`
    WHERE url NOT IN (SELECT url FROM `{PROJECT_ID}.{DATASET}.raw_stream_native`)
    """
    client.query(sync_sql).result()
    print("✅ 데이터 동기화 완료")

    # Step B: 본문이 없는 기사 180개 추출 및 업데이트
    query = f"SELECT url FROM `{PROJECT_ID}.{DATASET}.raw_stream_native` WHERE article_text IS NULL LIMIT 180"
    rows = client.query(query).result()

    for row in rows:
        try:
            content = trafilatura.extract(trafilatura.fetch_url(row.url))
            if content:
                update_sql = f"UPDATE `{PROJECT_ID}.{DATASET}.raw_stream_native` SET article_text = @content WHERE url = @url"
                job_config = bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("content", "STRING", content),
                    bigquery.ScalarQueryParameter("url", "STRING", row.url),
                ])
                client.query(update_sql, job_config=job_config).result()
                print(f"✔️ {row.url[:40]}... 본문 채움")
        except Exception as e:
            print(f"❌ 에러: {row.url} - {e}")

if __name__ == "__main__":

    run_pipeline()

