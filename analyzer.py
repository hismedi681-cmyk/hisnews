import os
import uuid
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
    """Gemini 답변에서 JSON 마크다운 태그(```json) 제거 및 순수 JSON 추출"""
    # ```json ... ``` 패턴을 찾아 내부 텍스트만 추출
    match = re.search(r'```json\s+(.*?)\s+```', text, re.DOTALL)
    if match:
        return match.group(1).strip()
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
    """분석된 맥락과 지표를 BigQuery에 삽입"""
    table_id = f"{project_id}.{DATASET}.{RESULT_TABLE}"
    
    # 델타와 위상 정보 추출
    physics = result.get('physics_engine', {})
    delta_score = physics.get('module_1_delta', {}).get('kl_divergence', 0.0)
    phase = physics.get('module_3_phase', {}).get('current_phase', 'UNKNOWN')
    
    # 내러티브 추출 (BQ 스키마 필드명에 맞춰 primary, counter, synthesis로 매핑)
    narratives = result.get('fmo_output', {}).get('module_5_narratives', {})
    
    row = {
        # 1. 고유 ID 생성 (REQUIRED 필드 충족)
        "analysis_id": str(uuid.uuid4()), 
        "title_hash": meta['title_hash'],
        "title": meta.get('title', 'Untitled'),
        "published_at": meta['published_at'].isoformat() if hasattr(meta['published_at'], 'isoformat') else meta['published_at'],
        "observed_at": datetime.datetime.now().isoformat(),
        
        # 2. 핵심 지표 (필터링용)
        "delta_score": float(delta_score),
        "phase": str(phase),
        
        # 3. 맥락 전체 보존 (JSON 타입)
        "analysis_payload": result, 
        
        # 4. 전략적 내러티브 (RECORD 타입 매핑)
        "strategic_narrative": {
            "primary": narratives.get("primary_narrative"),
            "counter": narratives.get("counter_narrative"),
            "synthesis": narratives.get("strategic_synthesis")
        }
    }
    
    errors = bq_client.insert_rows_json(table_id, [row])
    return errors

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
    ORDER BY published_at DESC LIMIT 5
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
