import os
import json
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig

# 1. 환경 변수 로드 (GitHub Secrets)
project_id = os.getenv("BQ_PROJECT_ID")
sa_json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
location = os.getenv("GCP_LOCATION", "asia-northeast3")

# 2. 인증 설정 (메모리 내에서 처리)
# 외부 시트를 읽지 않으므로 'cloud-platform' 스코프만으로 충분합니다.
scopes = ["https://www.googleapis.com/auth/cloud-platform"]
sa_info = json.loads(sa_json_str)
creds = service_account.Credentials.from_service_account_info(sa_info, scopes=scopes)

# 3. 각 서비스 초기화 (인증 객체 주입)
# BigQuery 클라이언트 생성
bq_client = bigquery.Client(credentials=creds, project=project_id)

# Vertex AI 초기화 (인증 객체 전달)
vertexai.init(project=project_id, location=location, credentials=creds)

# [설정 정보]
DATASET = "kinetic_field" # reader.py 예시와 맞춤
RAW_TABLE = "raw_stream_native"
RESULT_TABLE = "fmo_final_analysis"

# ------------------------------------------
# 분석 및 삽입 함수들 (이전과 로직은 동일하나 bq_client를 사용)
# ------------------------------------------

def analyze_article(article_text):
    """이중나선 동역학 엔진 실행"""
    model = GenerativeModel("gemini-1.5-pro")
    
    # engine_prompt.txt 로드 로직은 동일
    with open("engine_prompt.txt", "r", encoding="utf-8") as f:
        system_instruction = f.read()
    
    config = GenerationConfig(temperature=0.1, response_mime_type="application/json")
    prompt = f"{system_instruction}\n\n[기사]:\n{article_text}"
    
    response = model.generate_content(prompt, generation_config=config)
    return json.loads(response.text)

def insert_result(result, meta):
    """결과 삽입"""
    table_id = f"{project_id}.{DATASET}.{RESULT_TABLE}"
    
    row = {
        "analysis_id": f"fmo-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}",
        "title_hash": meta['title_hash'],
        "published_at": meta['published_at'].isoformat() if hasattr(meta['published_at'], 'isoformat') else meta['published_at'],
        "observed_at": datetime.datetime.now().isoformat(),
        "delta_score": result['physics_engine']['module_1_delta']['kl_divergence'],
        "phase": result['physics_engine']['module_3_phase']['current_phase'],
        "analysis_payload": result,
        "strategic_narrative": result['fmo_output']['module_5_narratives']
    }
    
    # 전역 bq_client 사용
    errors = bq_client.insert_rows_json(table_id, [row])
    return errors

# 4. 메인 파이프라인
def run_analyzer():
    # 미분석 기사 추출 (NOT EXISTS 로직)
    query = f"""
    SELECT article_text, title_hash, published_at, title
    FROM `{project_id}.{DATASET}.{RAW_TABLE}` AS raw
    WHERE NOT EXISTS (
        SELECT 1 FROM `{project_id}.{DATASET}.{RESULT_TABLE}` AS res 
        WHERE res.title_hash = raw.title_hash
    )
    AND article_text IS NOT NULL
    ORDER BY published_at DESC LIMIT 5
    """
    
    rows = bq_client.query(query).result()
    
    for row in rows:
        try:
            print(f"🧬 분석 중: {row.title[:30]}...")
            analysis_res = analyze_article(row.article_text)
            
            meta = {"title_hash": row.title_hash, "published_at": row.published_at}
            errors = insert_result(analysis_res, meta)
            
            if not errors:
                print(f"✅ 성공: {row.title_hash}")
            else:
                print(f"❌ 삽입 에러: {errors}")
        except Exception as e:
            print(f"⚠️ 실패: {e}")

if __name__ == "__main__":
    run_analyzer()
