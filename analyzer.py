import os
import uuid
import re
import json
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig

# 1. 환경 변수 로드
project_id = os.getenv("BQ_PROJECT_ID")
sa_json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
location = os.getenv("GCP_LOCATION", "asia-northeast3") # 서울 리전 유지

# 2. 인증 설정 (메모리 내 처리)
scopes = ["https://www.googleapis.com/auth/cloud-platform"]
sa_info = json.loads(sa_json_str)
creds = service_account.Credentials.from_service_account_info(sa_info, scopes=scopes)

# 3. 서비스 초기화
bq_client = bigquery.Client(credentials=creds, project=project_id)
vertexai.init(project=project_id, location=location, credentials=creds)

# [설정 정보]
DATASET = "kinetic_field"
RAW_TABLE = "raw_stream_native"
RESULT_TABLE = "fmo_final_analysis"

def clean_json_text(text):
    """Gemini 답변에서 순수 JSON 객체만 추출 (더 견고한 버전)"""
    try:
        # 1. 마크다운 태그 제거
        cleaned = re.sub(r'```json\s?|```', '', text).strip()
        
        # 2. 첫 '{'와 마지막 '}' 사이의 내용만 추출하여 사족 제거
        start_idx = cleaned.find('{')
        end_idx = cleaned.rfind('}')
        if start_idx != -1 and end_idx != -1:
            cleaned = cleaned[start_idx:end_idx+1]
        return cleaned
    except Exception:
        return text.strip()

def analyze_article(article_text):
    """Gemini 2.5 Flash를 사용하여 맥락적 동역학 분석 수행"""
    model = GenerativeModel("gemini-2.5-flash")
    
    with open("engine_prompt.txt", "r", encoding="utf-8") as f:
        system_instruction = f.read()
    
    config = GenerationConfig(
        temperature=0.1, 
        response_mime_type="application/json"
    )
    
    prompt = f"{system_instruction}\n\n[기사]:\n{article_text}"
    response = model.generate_content(prompt, generation_config=config)
    
    # JSON 텍스트 정제 후 파싱
    cleaned_json = clean_json_text(response.text)
    return json.loads(cleaned_json)

def insert_result(result, meta):
    table_id = f"{project_id}.{DATASET}.{RESULT_TABLE}"
    
    physics = result.get('physics_engine', {})
    narratives = result.get('fmo_output', {}).get('module_5_narratives', {})
    
    row = {
        "analysis_id": str(uuid.uuid4()),
        "title_hash": meta['title_hash'],
        "title": meta.get('title', 'Untitled'),
        "published_at": meta['published_at'].isoformat() if hasattr(meta['published_at'], 'isoformat') else meta['published_at'],
        "observed_at": datetime.datetime.now().isoformat(),
        
        "delta_score": float(physics.get('module_1_delta', {}).get('kl_divergence', 0.0)),
        "phase": str(physics.get('module_3_phase', {}).get('current_phase', 'UNKNOWN')),
        
        # [수정 포인트] JSON 타입 컬럼 에러 방지를 위해 dumps 사용
        "analysis_payload": json.dumps(result, ensure_ascii=False), 
        
        "strategic_narrative": {
            "primary": narratives.get("primary_narrative"),
            "counter": narratives.get("counter_narrative"),
            "synthesis": narratives.get("strategic_synthesis")
        }
    }
    
    # insert_rows_json은 리스트 형태의 행 데이터를 받습니다.
    return bq_client.insert_rows_json(table_id, [row])

def run_analyzer():
    # 미분석 기사 추출 (기존과 동일)
    query = f"""
    SELECT article_text, title_hash, published_at, title
    FROM `{project_id}.{DATASET}.{RAW_TABLE}` AS raw
    WHERE NOT EXISTS (
        SELECT 1 FROM `{project_id}.{DATASET}.{RESULT_TABLE}` AS res 
        WHERE res.title_hash = raw.title_hash
    )
    AND article_text IS NOT NULL
    ORDER BY published_at DESC LIMIT 50
    """
    
    rows = bq_client.query(query).result()
    
    for row in rows:
        try:
            print(f"🧬 분석 중: {row.title[:30]}...")
            analysis_res = analyze_article(row.article_text)
            
            # 메타데이터에 title 추가
            meta = {
                "title_hash": row.title_hash, 
                "published_at": row.published_at,
                "title": row.title
            }
            errors = insert_result(analysis_res, meta)
            
            if not errors:
                print(f"✅ 성공: {row.title[:20]}...")
            else:
                print(f"❌ 삽입 에러: {errors}")
        except Exception as e:
            print(f"⚠️ 실패: {e}")

if __name__ == "__main__":
    run_analyzer()
