import mysql.connector
from mysql.connector import Error
import time
import os

DB_CONFIG = {
    # 'host': os.getenv('DB_HOST'),
    # 'database': os.getenv('DB_NAME'),
    # 'user': os.getenv('DB_USER'),
    # 'password': os.getenv('DB_PASSWORD')
    'database': 'legacy',
    'user': 'root',
    'password': 'n9800211'
}
# 값
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

LINE_RULES = [
    {'keyword': '삼국', 'store_template': '삼국시대 팩', 'attribute': '역사'},
    {'keyword': '경', 'store_template': '역사&학문 팩', 'attribute': '학문'},
    {'keyword': '책', 'store_template': '역사&학문 팩', 'attribute': '학문'},
    {'keyword': '서원', 'store_template': '역사&학문 팩', 'attribute': '학문'},
    {'keyword': '향교', 'store_template': '역사&학문 팩', 'attribute': '학문'},
    {'keyword': '성균관', 'store_template': '역사&학문 팩', 'attribute': '학문'},
    {'keyword': '사', 'store_template': '신앙&기술 팩', 'attribute': '신앙'},
    {'keyword': '암', 'store_template': '신앙&기술 팩', 'attribute': '신앙'},
    {'keyword': '탑', 'store_template': '신앙&기술 팩', 'attribute': '신앙'},
    {'keyword': '부도', 'store_template': '신앙&기술 팩', 'attribute': '신앙'},
    {'keyword': '사지', 'store_template': '신앙&기술 팩', 'attribute': '신앙'},
    {'keyword': '석불', 'store_template': '신앙&기술 팩', 'attribute': '신앙'},
    {'keyword': '미륵', 'store_template': '신앙&기술 팩', 'attribute': '신앙'},
    {'keyword': '당간지주', 'store_template': '신앙&기술 팩', 'attribute': '신앙'},
    {'keyword': '궁', 'attribute': '상징'}, {'keyword': '릉', 'attribute': '상징'},
    {'keyword': '총', 'attribute': '상징'}, {'keyword': '원', 'attribute': '상징'},
    {'keyword': '묘', 'attribute': '상징'}, {'keyword': '문', 'attribute': '체제'},
    {'keyword': '대', 'attribute': '놀이'}, {'keyword': '루', 'attribute': '놀이'},
    {'keyword': '정', 'attribute': '놀이'}, {'keyword': '성', 'attribute': '체제'},
    {'keyword': '성곽', 'attribute': '체제'}, {'keyword': '읍성', 'attribute': '체제'},
    {'keyword': '진', 'attribute': '체제'}, {'keyword': '보', 'attribute': '체제'},
    {'keyword': '돈대', 'attribute': '체제'}, {'keyword': '봉수대', 'attribute': '체제'},
    {'keyword': '고분', 'attribute': '의식주'}, {'keyword': '가마터', 'attribute': '기술'},
    {'keyword': '집터', 'attribute': '의식주'}, {'keyword': '고인돌', 'attribute': '의식주'},
    {'keyword': '선돌', 'attribute': '의식주'},
]
# 기본 값 설정
def get_line_attribute_from_rules(ruin_name):
    """유적 이름을 보고 규칙 기반으로 '계열' 속성을 판단하는 함수"""
    if ruin_name:
        for rule in LINE_RULES:
            if rule['keyword'] in ruin_name:
                print(f"   [규칙 분석] '{ruin_name}' -> '{rule['attribute']}'")
                return rule['attribute']

    print(f"   [기본값] '{ruin_name}' -> '역사'")
    return '역사'


def get_region_attribute_from_rules(detail_address, category):
    if detail_address:
        for rule in REGION_RULES:
            if rule['keyword'] in detail_address:
                print(f"   [규칙 분석] '{detail_address}' -> '{rule['attribute']}'")
                return rule['attribute']

    if category:
        for rule in REGION_RULES:
            if rule['keyword'] in category:
                print(f"   [규칙 분석] '{category}' -> '{rule['attribute']}'")
                return rule['attribute']

    print(f"   [기본값] 지역 정보 없음 -> '경기' (서울)")
    return '경기'


def get_nation_attribute_from_rules(period_name):
    if period_name:
        for rule in NATION_RULES:
            if rule['keyword'] in period_name:
                print(f"   [규칙 분석] '{period_name}' -> '{rule['attribute']}'")
                return rule['attribute']
    # 기본값
    print(f"   [기본값] 시대 정보 없음 -> '대한제국'")
    return '대한제국'


def get_mappings(cursor):
    mappings = {}

    cursor.execute("SELECT store_id, store_name FROM store")
    mappings['store'] = {name: id for id, name in cursor.fetchall()}

    cursor.execute("SELECT nation_attribute_id, attribute_name FROM nation_attribute")
    mappings['nation'] = {name.strip(): id for id, name in cursor.fetchall()}

    cursor.execute("SELECT line_attribute_id, attribute_name FROM line_attribute")
    mappings['line'] = {name: id for id, name in cursor.fetchall()}

    cursor.execute("SELECT region_attribute_id, attribute_name FROM region_attribute")
    mappings['region'] = {name: id for id, name in cursor.fetchall()}

    return mappings


def determine_card_properties(ruin_name, period_name, detail_address, category):
    store_name = None
    nation_attr_name = None
    region_attr_name = None
    line_attr_name = None

    if period_name:
        for rule in NATION_RULES:
            if rule['keyword'] in period_name:
                store_name = rule['store_template']
                nation_attr_name = rule['attribute']
                break

    if ruin_name:
        for rule in LINE_RULES:
            if rule['keyword'] in ruin_name:
                line_attr_name = rule['attribute']
                if rule['keyword'] == '삼국':
                    store_name = rule['store_template']
                break

    region_attr_name = get_region_attribute_from_rules(detail_address, category)

    if not store_name:
        if line_attr_name in ["학문", "역사"]:
            store_name = "역사&학문 팩"
        elif line_attr_name in ["기술", "신앙"]:
            store_name = "신앙&기술 팩"
        elif line_attr_name in ["체제", "상징"]:
            store_name = "신앙&체제 팩"
        elif line_attr_name in ["놀이", "의식주"]:
            store_name = "놀이&의식주 팩"

    if not nation_attr_name:
        nation_attr_name = '대한제국'

    if not line_attr_name:
        line_attr_name = '역사'

    return {
        'store_name': store_name,
        'nation_attribute_name': nation_attr_name,
        'region_attribute_name': region_attr_name,
        'line_attribute_name': line_attr_name
    }


def generate_cards():
    connection = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        mappings = get_mappings(cursor)
        print("✅ 매핑 정보 로딩 완료")

        cursor.execute("SELECT ruins_id, name, ruins_image, period_name, detail_address, category FROM ruins")
        ruins_data = cursor.fetchall()
        print(f"🏛️ {len(ruins_data)}개의 유적 데이터 조회 완료. 규칙 기반 분석을 시작합니다...")

        cards_to_insert = []

        for ruin_id, ruin_name, ruin_image, period_name, detail_address, category in ruins_data:
            properties = determine_card_properties(ruin_name, period_name, detail_address, category)

            store_name = properties['store_name']
            nation_attr_name = properties['nation_attribute_name']
            region_attr_name = properties['region_attribute_name']
            line_attr_name = properties['line_attribute_name']

            store_id = mappings['store'].get(store_name)
            if not store_id:
                print(f"⚠️ 경고: '{ruin_name}'에 대한 store '{store_name}'를 찾을 수 없습니다. 건너뜁니다.")
                continue

            nation_attr_id = mappings['nation'].get(nation_attr_name.strip())
            region_attr_id = mappings['region'].get(region_attr_name.strip())
            line_attr_id = mappings['line'].get(line_attr_name.strip())

            card_data = (
                ruin_id, ruin_name, ruin_image, store_id,
                region_attr_id, nation_attr_id, line_attr_id
            )
            cards_to_insert.append(card_data)

        if cards_to_insert:
            sql = """
                  INSERT INTO card (ruins_id, card_name, card_image_url, store_id, \
                                    region_attribute_id, nation_attribute_id, line_attribute_id) \
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

# 실행
if __name__ == '__main__':
    generate_cards()