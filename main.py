import mysql.connector
from mysql.connector import Error
import time
import os

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
    # 'database': 'legacy',
    # 'user': 'root',
    # 'password': 'n9800211'
}

# 시대
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

# 지역
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

# 계열
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

DEFAULT_STORE = '역사&학문 팩'
DEFAULT_NATION = '대한제국'
DEFAULT_REGION = '서울'
DEFAULT_LINE = '역사'


def get_mappings(cursor):
    """DB의 모든 매핑 정보를 가져오는 함수"""
    mappings = {}

    cursor.execute("SELECT store_id, store_name FROM store")
    mappings['store'] = {name: id for id, name in cursor.fetchall()}

    cursor.execute("SELECT nation_attribute_id, attribute_name FROM nation_attribute")
    mappings['nation'] = {name.strip(): id for id, name in cursor.fetchall()}

    cursor.execute("SELECT line_attribute_id, attribute_name FROM line_attribute")
    mappings['line'] = {name.strip(): id for id, name in cursor.fetchall()}

    cursor.execute("SELECT region_attribute_id, attribute_name FROM region_attribute")
    mappings['region'] = {name.strip(): id for id, name in cursor.fetchall()}

    return mappings


def get_existing_card_ruins_ids(cursor):
    """이미 카드로 생성된 유적 ID 목록을 가져오는 함수"""
    cursor.execute("SELECT ruins_id FROM card")
    existing_ids = {row[0] for row in cursor.fetchall()}
    return existing_ids


def determine_card_properties(ruin_name, period_name, detail_address, category, mappings):
    store_name = None
    nation_attr_name = None
    region_attr_name = None
    line_attr_name = None

    #시대 정보로 store와 nation
    if period_name:
        for rule in NATION_RULES:
            if rule['keyword'] in period_name:
                store_name = rule['store_template']
                nation_attr_name = rule['attribute']
                print(f"   [시대 분석] '{period_name}' -> store: '{store_name}', nation: '{nation_attr_name}'")
                break

    #유적 이름으로 계열(line) 결정
    if ruin_name:
        for rule in LINE_RULES:
            if rule['keyword'] in ruin_name:
                line_attr_name = rule['attribute']
                if rule['keyword'] == '삼국' and not store_name:
                    store_name = rule['store_template']
                print(f"   [계열 분석] '{ruin_name}' -> line: '{line_attr_name}'")
                break

    #주소/카테고리로 지역 결정
    if detail_address:
        for rule in REGION_RULES:
            if rule['keyword'] in detail_address:
                region_attr_name = rule['attribute']
                print(f"   [지역 분석] '{detail_address}' -> region: '{region_attr_name}'")
                break

    if not region_attr_name and category:
        for rule in REGION_RULES:
            if rule['keyword'] in category:
                region_attr_name = rule['attribute']
                print(f"   [카테고리 분석] '{category}' -> region: '{region_attr_name}'")
                break

    #store가 없으면 계열에 따라 결정
    if not store_name and line_attr_name:
        if line_attr_name in ["학문", "역사"]:
            store_name = "역사&학문 팩"
        elif line_attr_name in ["기술", "신앙"]:
            store_name = "신앙&기술 팩"
        elif line_attr_name in ["체제", "상징"]:
            store_name = "신앙&체제 팩"
        elif line_attr_name in ["놀이", "의식주"]:
            store_name = "놀이&의식주 팩"
        print(f"   [Store 추론] line '{line_attr_name}' -> store: '{store_name}'")

    #기본값 적용 (무조건 값이 있도록 보장)
    if not store_name:
        store_name = DEFAULT_STORE
        print(f"   [기본값] store -> '{DEFAULT_STORE}'")

    if not nation_attr_name:
        nation_attr_name = DEFAULT_NATION
        print(f"   [기본값] nation -> '{DEFAULT_NATION}'")

    if not region_attr_name:
        region_attr_name = DEFAULT_REGION
        print(f"   [기본값] region -> '{DEFAULT_REGION}'")

    if not line_attr_name:
        line_attr_name = DEFAULT_LINE
        print(f"   [기본값] line -> '{DEFAULT_LINE}'")

    #매핑 테이블에서 ID 찾기
    store_id = mappings['store'].get(store_name)
    nation_attr_id = mappings['nation'].get(nation_attr_name.strip())
    region_attr_id = mappings['region'].get(region_attr_name.strip())
    line_attr_id = mappings['line'].get(line_attr_name.strip())

    # 7. ID가 없으면 첫 번째 항목 사용
    if not store_id and mappings['store']:
        store_id = list(mappings['store'].values())[0]
        print(f"   [경고] store ID를 찾을 수 없어 첫 번째 store 사용")

    if not nation_attr_id and mappings['nation']:
        nation_attr_id = list(mappings['nation'].values())[0]
        print(f"   [경고] nation ID를 찾을 수 없어 첫 번째 nation 사용")

    if not region_attr_id and mappings['region']:
        region_attr_id = list(mappings['region'].values())[0]
        print(f"   [경고] region ID를 찾을 수 없어 첫 번째 region 사용")

    if not line_attr_id and mappings['line']:
        line_attr_id = list(mappings['line'].values())[0]
        print(f"   [경고] line ID를 찾을 수 없어 첫 번째 line 사용")

    return {
        'store_id': store_id,
        'nation_attr_id': nation_attr_id,
        'region_attr_id': region_attr_id,
        'line_attr_id': line_attr_id
    }


def generate_cards():
    connection = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        mappings = get_mappings(cursor)
        print("매핑 정보 로딩 완료")
        print(f"   - Store: {len(mappings['store'])}개")
        print(f"   - Nation: {len(mappings['nation'])}개")
        print(f"   - Region: {len(mappings['region'])}개")
        print(f"   - Line: {len(mappings['line'])}개")

        # 이미 카드로 생성된 유적 ID 가져오기
        existing_card_ruins_ids = get_existing_card_ruins_ids(cursor)
        print(f"이미 생성된 카드: {len(existing_card_ruins_ids)}개")

        # 전체 유적 데이터 조회
        cursor.execute("SELECT ruins_id, name, ruins_image, period_name, detail_address, category FROM ruins")
        ruins_data = cursor.fetchall()
        print(f"전체 유적: {len(ruins_data)}개")

        # 카드가 없는 유적만 필터링
        new_ruins = [ruin for ruin in ruins_data if ruin[0] not in existing_card_ruins_ids]
        print(f"생성할 새로운 카드: {len(new_ruins)}개")

        if not new_ruins:
            print("모든 유적이 이미 카드로 생성되어 있습니다!")
            return

        cards_to_insert = []
        skipped_count = 0

        for ruin_id, ruin_name, ruin_image, period_name, detail_address, category in new_ruins:
            print(f"\n[{len(cards_to_insert) + 1}/{len(new_ruins)}] 유적 분석: {ruin_name} (ID: {ruin_id})")

            properties = determine_card_properties(
                ruin_name, period_name, detail_address, category, mappings
            )

            # 모든 ID가 있는지 확인
            if not all([properties['store_id'], properties['nation_attr_id'],
                        properties['region_attr_id'], properties['line_attr_id']]):
                print(f"필수 속성을 찾을 수 없어 건너뜁니다.")
                print(f"      store_id: {properties['store_id']}, nation: {properties['nation_attr_id']}, "
                      f"region: {properties['region_attr_id']}, line: {properties['line_attr_id']}")
                skipped_count += 1
                continue

            card_data = (
                ruin_id,
                ruin_name,
                ruin_image,
                properties['store_id'],
                properties['region_attr_id'],
                properties['nation_attr_id'],
                properties['line_attr_id']
            )
            cards_to_insert.append(card_data)
            print(f"카드 생성 준비 완료")

        if cards_to_insert:
            sql = """
                  INSERT INTO card (ruins_id, card_name, card_image_url, store_id,
                                    region_attribute_id, nation_attribute_id, line_attribute_id)
                  VALUES (%s, %s, %s, %s, %s, %s, %s) \
                  """
            cursor.executemany(sql, cards_to_insert)
            connection.commit()
            print(f"\n카드 {cursor.rowcount}개가 성공적으로 생성되었습니다!")
            if skipped_count > 0:
                print(f"건너뛴 유적: {skipped_count}개")
        else:
            print(f"\n생성할 수 있는 카드가 없습니다. (건너뛴 유적: {skipped_count}개)")

    except Error as e:
        print(f"DB 오류 발생: {e}")
        if connection:
            connection.rollback()
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("\nMySQL 연결이 종료되었습니다.")


if __name__ == '__main__':
    generate_cards()