import google.generativeai as genai
import mysql.connector
from mysql.connector import Error
import time
import os


GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel('gemini-1.0-pro')

NATION_RULES = [
    {'keyword': '삼국', 'store_template': '삼국시대 팩', 'attribute': '역사'},
    {'keyword': '고구려', 'store_template': '삼국시대 팩', 'attribute': '고구려'},
    {'keyword': '신라', 'store_template': '삼국시대 팩', 'attribute': '신라'},
    {'keyword': '백제', 'store_template': '삼국시대 팩', 'attribute': '백제'},
    {'keyword': '통일신라', 'store_template': '삼국시대 팩', 'attribute': '통일신라'},
    {'keyword': '고려', 'store_template': '고려시대 팩', 'attribute': '고려'},
    {'keyword': '조선', 'store_template': '조선시대 팩', 'attribute': '조선'},
    {'keyword': '대한제국', 'store_template': '대한민국 팩', 'attribute': '대한제국'},
    {'keyword': '일제강점기', 'store_template': '대한민국 팩', 'attribute': '대한민국'},
]
REGION_RULES = [
    {'keyword': '서울', 'store_template': '경기도 팩', 'attribute': '경기'},
    {'keyword': '부산', 'store_template': '경상도 팩', 'attribute': '경남'},
    {'keyword': '대구', 'store_template': '경상도 팩', 'attribute': '경북'},
    {'keyword': '인천', 'store_template': '경기도 팩', 'attribute': '경기'},
    {'keyword': '광주', 'store_template': '전라도 팩', 'attribute': '전남'},
    {'keyword': '대전', 'store_template': '충청도 팩', 'attribute': '충남'},
    {'keyword': '울산', 'store_template': '경상도 팩', 'attribute': '경남'},
    {'keyword': '세종', 'store_template': '충청도 팩', 'attribute': '충남'},
    {'keyword': '경기', 'store_template': '경기도 팩', 'attribute': '경기'},
    {'keyword': '강원', 'store_template': '강원도 팩', 'attribute': '강원'},
    {'keyword': '충북', 'store_template': '충청도 팩', 'attribute': '충북'},
    {'keyword': '충남', 'store_template': '충청도 팩', 'attribute': '충남'},
    {'keyword': '전북', 'store_template': '전라도 팩', 'attribute': '전북'},
    {'keyword': '전남', 'store_template': '전라도 팩', 'attribute': '전남'},
    {'keyword': '경북', 'store_template': '경상도 팩', 'attribute': '경북'},
    {'keyword': '경남', 'store_template': '경상도 팩', 'attribute': '경남'},
    {'keyword': '제주', 'store_template': '제주도 팩', 'attribute': '제주'},
]

def get_line_attribute_from_ai(ruin_name):
    """AI에게 유적 이름을 보내 '계열' 속성을 직접 물어보는 함수"""
    try:
        prompt = f"""당신은 한국 문화재를 분류하는 역사 전문가입니다.
        주어진 문화재 이름을 보고 다음 보기 중에서 가장 적합한 분류를 단 하나만 선택하세요.
        보기: [상징, 신앙, 학문, 체제, 놀이, 기술, 의식주]
        다른 설명은 일절 추가하지 말고, 오직 보기에 있는 단어 하나만 한국어로 답변해야 합니다.
        문화재 이름: "{ruin_name}"
        """
        response = model.generate_content(prompt)
        clean_response = response.text.strip()

        valid_categories = ["상징", "신앙", "학문", "체제", "놀이", "기술", "의식주"]
        if clean_response in valid_categories:
            print(f"   [AI 분석] '{ruin_name}' -> '{clean_response}'")
            return clean_response
        else:
            print(f"   [AI 경고] '{ruin_name}'에 대한 AI의 답변이 유효하지 않음: {clean_response}")
            return None

    except Exception as e:
        print(f"   [AI 오류] API 호출 중 문제 발생: {e}")
        return None


def get_mappings(cursor):
    """DB에서 store, attribute 정보를 가져와 파이썬 딕셔너리로 변환합니다."""
    mappings = {}

    cursor.execute("SELECT store_id, store_name FROM store")
    mappings['store'] = {name: id for id, name in cursor.fetchall()}

    cursor.execute("SELECT nation_attribute_id, attribute_name FROM nation_attribute")
    mappings['nation'] = {name: id for id, name in cursor.fetchall()}

    cursor.execute("SELECT line_attribute_id, attribute_name FROM line_attribute")
    mappings['line'] = {name: id for id, name in cursor.fetchall()}

    cursor.execute("SELECT region_attribute_id, attribute_name FROM region_attribute")
    mappings['region'] = {name: id for id, name in cursor.fetchall()}

    return mappings


def determine_card_properties(ruin_name, period_name, detail_address):
    """(시대 -> 지역)은 규칙으로, (계열)은 AI API로 판단하여 속성을 누적합니다."""
    store_name = None
    attributes = []

    if period_name:
        for rule in NATION_RULES:
            if rule['keyword'] in period_name:
                store_name = rule['store_template']
                attributes.append(rule['attribute'])
                break

    if detail_address:
        for rule in REGION_RULES:
            if rule['keyword'] in detail_address:
                if not store_name:
                    store_name = rule['store_template']
                attributes.append(rule['attribute'])
                break

    line_attribute = get_line_attribute_from_ai(ruin_name)
    if line_attribute:
        attributes.append(line_attribute)
        # ▼▼▼▼▼ [핵심 수정] store가 없을 때, AI가 판단한 계열에 따라 store를 할당 ▼▼▼▼▼
        if not store_name:
            if line_attribute == "학문":
                store_name = "역사&학문 팩"
            elif line_attribute == "기술":
                store_name = "신앙&기술 팩"
            elif line_attribute == "체제":
                store_name = "신앙&체제 팩"  # 이미지에 맞춰 수정
            elif line_attribute in ["놀이", "의식주"]:
                store_name = "놀이&의식주 팩"
            else:
                store_name = "신앙&체제 팩"  # 그 외 나머지는 기본값으로 할당

    unique_attributes = list(dict.fromkeys(attributes))

    return {
        'store_name': store_name,
        'attributes': unique_attributes
    }


def generate_cards():
    """메인 실행 함수"""
    connection = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        mappings = get_mappings(cursor)
        print("✅ 매핑 정보 로딩 완료")

        cursor.execute("SELECT ruins_id, name, ruins_image, period_name, detail_address FROM ruins")
        ruins_data = cursor.fetchall()
        print(f"🏛️ {len(ruins_data)}개의 유적 데이터 조회 완료. AI 분석을 시작합니다...")

        cards_to_insert = []

        for ruin_id, ruin_name, ruin_image, period_name, detail_address in ruins_data:
            properties = determine_card_properties(ruin_name, period_name, detail_address)
            time.sleep(1)

            store_name = properties['store_name']
            attribute_names = properties['attributes']

            store_id = mappings['store'].get(store_name)
            if not store_id:
                print(f"⚠️ 경고: '{ruin_name}'에 대한 store '{store_name}'를 찾을 수 없습니다. 건너뜁니다.")
                continue

            attr_ids = []
            for attr_name in attribute_names[:3]:
                attr_id = mappings['nation'].get(attr_name) or \
                          mappings['line'].get(attr_name) or \
                          mappings['region'].get(attr_name)
                if attr_id:
                    attr_ids.append(attr_id)

            attr_ids.extend([None] * (3 - len(attr_ids)))

            card_data = (
                ruin_id, ruin_name, ruin_image, store_id,
                attr_ids[0], attr_ids[1], attr_ids[2]
            )
            cards_to_insert.append(card_data)

        if cards_to_insert:
            sql = """
                  INSERT INTO card (ruins_id, card_name, card_image_url, store_id, \
                                    attribute_1_id, attribute_2_id, attribute_3_id) \
                  VALUES (%s, %s, %s, %s, %s, %s, %s) \
                  """
            cursor.executemany(sql, cards_to_insert)
            connection.commit()
            print(f"🎉 카드 {cursor.rowcount}개가 성공적으로 생성되었습니다!")
        else:
            print("생성할 카드가 없습니다.")

    except Error as e:
        print(f"DB 오류 발생: {e}")
        if connection: connection.rollback()
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL 연결이 종료되었습니다.")


if __name__ == '__main__':
    generate_cards()