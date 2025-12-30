import os
import json
from google.cloud import bigquery
import trafilatura
from google.oauth2 import service_account

# 1. 환경 변수에서 설정값 로드
# YAML의 env 섹션에 정의된 이름과 정확히 일치해야 합니다.
target_project_id = os.getenv("BQ_PROJECT_ID")
sa_json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

if not target_project_id or not sa_json_str:
    raise ValueError("❌ 환경 변수(BQ_PROJECT_ID 또는 GOOGLE_SERVICE_ACCOUNT_JSON)가 설정되지 않았습니다.")

# 2. 인증 및 클라이언트 설정
sa_info = json.loads(sa_json_str)
creds = service_account.Credentials.from_service_account_info(sa_info)

# ★ 핵심: 인증 정보가 어떤 프로젝트 것이든, 실제 작업은 target_project_id에서 수행합니다.
client = bigquery.Client(credentials=creds, project=target_project_id)

DATASET = "kinetic_field"

def run_pipeline():
    # Step A: 시트 데이터 동기화
    print(f"🔄 [{target_project_id}] 프로젝트 데이터 동기화 중...")
    sync_sql = f"""
    INSERT INTO `{target_project_id}.{DATASET}.raw_stream_native` 
    (published_at, source, title, url, url_canonical, tags, title_hash, simhash, duplicate_of)
    SELECT published_at, source, title, url, url_canonical, tags, title_hash, simhash, duplicate_of
    FROM `{target_project_id}.{DATASET}.raw_stream_entry`
    WHERE url NOT IN (SELECT url FROM `{target_project_id}.{DATASET}.raw_stream_native`)
    """
    client.query(sync_sql).result()

    # Step B: 본문 추출 및 업데이트 (LIMIT 180)
    query = f"SELECT url FROM `{target_project_id}.{DATASET}.raw_stream_native` WHERE article_text IS NULL LIMIT 180"
    rows = client.query(query).result()

    for row in rows:
        try:
            # 타임아웃 10초 설정으로 무한 대기 방지
            res = trafilatura.fetch_url(row.url)
            content = trafilatura.extract(res) if res else None
            
            if content:
                update_sql = f"UPDATE `{target_project_id}.{DATASET}.raw_stream_native` SET article_text = @content WHERE url = @url"
                job_config = bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("content", "STRING", content),
                    bigquery.ScalarQueryParameter("url", "STRING", row.url),
                ])
                client.query(update_sql, job_config=job_config).result()
                print(f"✔️ 성공: {row.url[:50]}...")
        except Exception as e:
            print(f"❌ 실패: {row.url[:50]} - {e}")

if __name__ == "__main__":
    run_pipeline()
